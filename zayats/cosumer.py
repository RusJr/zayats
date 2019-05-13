import json
import logging.config
from _socket import gaierror
from time import sleep
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from queue import Queue, Empty
from threading import Thread

from zayats.utils import set_logger


class StopConsuming:
    pass


class RabbitConsumer:

    acknowledge_period = 10  # seconds
    check_connection_period = 3  # seconds
    default_reconnect_sleep = 10  # seconds

    def __init__(self, pika_params: pika.ConnectionParameters,
                 queue: str,
                 exchange='',
                 exchange_type='',
                 lazy_connection=True,
                 reconnect_sleep=default_reconnect_sleep,
                 logging_level='INFO'):

        # logging ------------------------------------
        _logger_name = type(self).__name__
        set_logger(_logger_name, logging_level)
        self._logger = logging.getLogger(_logger_name)

        self.reconnect_sleep = reconnect_sleep

        self._pika_params = pika_params
        self._pika_queue = queue
        self.exchange = exchange
        self.exchange_type = exchange_type

        self._pika_connection = None
        self._pika_channel = None
        if not lazy_connection:
            self._check_connection_and_channel()

        self._current_task = None
        self._python_q_task = Queue()
        self._python_q_acknowledge = Queue()

        self._consuming_thread = None

    def __del__(self):
        if hasattr(self, '_pika_connection') and self._pika_connection and not self._pika_connection.is_closed:
            self._pika_connection.close()

    @property
    def pika_connection(self) -> pika.BlockingConnection:
        self._check_connection_and_channel()
        return self._pika_connection

    @property
    def pika_channel(self) -> BlockingChannel:
        self._check_connection_and_channel()
        return self._pika_channel

    def send_ack_and_get_new_msg(self, timeout=None) -> Any:
        self._check_consuming_thread()
        self.send_ack()

        _timeout = min(self.check_connection_period, timeout) if timeout else self.check_connection_period
        _total_spent_time = 0
        while not timeout or _total_spent_time < timeout:
            try:
                self._current_task = self._python_q_task.get(timeout=_timeout)
                return self._current_task
            except Empty:
                self._check_consuming_thread()
                if timeout:
                    _total_spent_time += _timeout

        self._logger.debug('No messages (timeout)')
        return None  # if timeout

    def send_ack(self, stop_consuming=False) -> None:
        if self._current_task is not None:
            self._python_q_acknowledge.put('done')
            self._current_task = None

        if stop_consuming:
            self.stop_consuming()

    def stop_consuming(self) -> None:
        self._python_q_acknowledge.put(StopConsuming)

        self._current_task = None
        while not self._python_q_task.empty():
            self._python_q_task.get()

        self._logger.debug('consuming stopped')

    def _check_connection_and_channel(self):
        while not self._pika_connection or self._pika_connection.is_closed:
            try:
                self._pika_connection = pika.BlockingConnection(parameters=self._pika_params)
            except (AMQPConnectionError, gaierror) as e:
                self._logger.critical('Connection problem: %s(%s). Retry after %d seconds',
                                      type(e).__name__, e, self.reconnect_sleep)
                sleep(self.reconnect_sleep)
            else:
                self._logger.info('Connected with RabbitMQ(%s:%s)', self._pika_params.host, self._pika_params.port)

        if not self._pika_channel or self._pika_channel.is_closed:
            self._pika_channel = self._pika_connection.channel()
            self._pika_channel.basic_qos(prefetch_count=1)
            self._pika_channel.queue_declare(queue=self._pika_queue, durable=True)
            if self.exchange:
                self._pika_channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
                self._pika_channel.queue_bind(exchange=self.exchange, queue=self._pika_queue)
                self._logger.info('Created a new instance of the Channel')

    def _wait_ack(self):
        while True:
            try:
                ack = self._python_q_acknowledge.get(timeout=self.acknowledge_period)  # waiting for ack order
                return ack
            except Empty:
                self._pika_connection.process_data_events()

    def _consuming_callback(self, ch, method, properties, body):
        try:
            received_task = json.loads(body.decode())
        except json.JSONDecodeError:
            self._logger.error('[RabbitConsumer] Task skipped. JSONDecodeError on "%s"', body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self._python_q_task.put(received_task)
            self._logger.debug('Got msg: %s', received_task)

            ack = self._wait_ack()
            if ack is StopConsuming:
                ch.basic_reject(delivery_tag=method.delivery_tag)
                self._pika_channel.stop_consuming()
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def _check_consuming_thread(self):
        self._check_connection_and_channel()
        if not self._consuming_thread or not self._consuming_thread.is_alive():

            # clean
            self._current_task = None
            while not self._python_q_task.empty():
                self._python_q_task.get()

            # new thread
            self._pika_channel.basic_consume(self._pika_queue, self._consuming_callback, auto_ack=False)
            self._consuming_thread = Thread(target=self._pika_channel.start_consuming)
            self._consuming_thread.daemon = True
            self._consuming_thread.start()
            self._logger.debug('Consuming thread has been started')
