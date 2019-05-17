import json
import logging.config
from _socket import gaierror
from time import sleep
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from queue import Queue, Empty
from threading import Thread, Event

from zayats.utils import set_logger


class SendAcknowledgeSignal:
    pass


class RejectSignal:
    pass


class RabbitConsumer:
    """ For consuming only. Do not use '__connection' """

    acknowledge_period = 5  # seconds
    check_connection_period = 1  # seconds
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

        self.__pika_connection: pika.BlockingConnection = None
        self.__pika_channel: BlockingChannel = None
        if not lazy_connection:
            self._check_connection_and_channel()

        self._current_task = None
        self._thread_input = Queue()
        self._thread_output = Queue()

        self._consuming_thread: Thread = None
        self._stop_event = Event()

    def __del__(self):
        if hasattr(self, '_pika_connection') and self.__pika_connection and not self.__pika_connection.is_closed:
            self.__pika_connection.close()

    @property
    def is_consuming(self) -> bool:
        return self._thread_is_alive()

    def send_ack_and_get_new_msg(self, timeout=None) -> Any:
        self._check_thread()
        self.send_ack()

        _timeout = min(self.check_connection_period, timeout) if timeout else self.check_connection_period
        _total_spent_time = 0
        while not timeout or _total_spent_time < timeout:
            try:
                self._current_task = self._thread_output.get(timeout=_timeout)
                return self._current_task
            except Empty:
                self._check_thread()
                if timeout:
                    _total_spent_time += _timeout

        self._logger.debug('No messages (timeout)')
        return None  # if timeout

    def send_ack(self, stop_consuming=False) -> None:
        if not self._stop_event.is_set() and not self.is_consuming:
            self._logger.error('Acknowledge sending error: connection lost')

        if self._current_task is not None:
            self._thread_input.put(SendAcknowledgeSignal)
            self._current_task = None

        if stop_consuming:
            self.stop_consuming()

    def stop_consuming(self) -> None:
        if self.is_consuming:
            self._stop_event.set()
            self._thread_input.put(RejectSignal)
            self._consuming_thread.join()

            # clear
            self._current_task = None

    def _check_connection_and_channel(self):
        while not self.__pika_connection or self.__pika_connection.is_closed:
            try:
                self.__pika_connection = pika.BlockingConnection(parameters=self._pika_params)
            except (AMQPConnectionError, gaierror) as e:
                self._logger.error('Connection problem: %s(%s). Retry after %d seconds',
                                   type(e).__name__, e, self.reconnect_sleep)
                sleep(self.reconnect_sleep)
            else:
                self._logger.info('Connected with RabbitMQ(%s:%s)', self._pika_params.host, self._pika_params.port)

        if not self.__pika_channel or self.__pika_channel.is_closed:
            self.__pika_channel = self.__pika_connection.channel()
            self.__pika_channel.basic_qos(prefetch_count=1)
            self.__pika_channel.queue_declare(queue=self._pika_queue, durable=True)
            if self.exchange:
                self.__pika_channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
                self.__pika_channel.queue_bind(exchange=self.exchange, queue=self._pika_queue)
                self._logger.info('Created a new instance of the Channel')

    def _consuming_callback(self, ch: BlockingChannel, method, properties, body):
        try:
            rabbit_message = json.loads(body.decode())
        except json.JSONDecodeError:
            self._logger.error('[RabbitConsumer] Task skipped. JSONDecodeError on "%s"', body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self._thread_output.put(rabbit_message)
            self._logger.debug('Got msg: %s', rabbit_message)

            # waiting for main thread
            signal = self._wait_signal()
            if signal is SendAcknowledgeSignal:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            elif signal is RejectSignal:
                ch.basic_reject(delivery_tag=method.delivery_tag)
            else:
                raise Exception('Excuse me what the type?')

    def _wait_signal(self):
        while True:
            try:
                ack = self._thread_input.get(timeout=self.acknowledge_period)  # waiting for ack order
                return ack
            except Empty:
                self.__pika_connection.process_data_events()

    def _thread_callback(self):
        self._logger.debug('[Consuming thread] Started')
        self._stop_event.clear()
        try:
            self.__pika_channel.basic_consume(self._pika_queue, self._consuming_callback, auto_ack=False)
            while True:
                if self._stop_event.is_set():
                    self.__pika_channel.stop_consuming()
                    break
                self.__pika_connection.process_data_events(0.5)  # start_consuming analog

        except Exception as e:
            self._logger.warning('[Consuming thread] Stopped. Error: %s(%s)', type(e).__name__, e)
        else:
            self._logger.debug('[Consuming thread] Stopped. OK')

    def _thread_is_alive(self) -> bool:
        return self._consuming_thread and self._consuming_thread.is_alive()

    def _check_thread(self):
        self._check_connection_and_channel()
        if not self._thread_is_alive():

            # clear
            self._current_task = None
            while not self._thread_input.empty():
                self._thread_input.get()
            while not self._thread_output.empty():
                self._thread_output.get()

            self._consuming_thread = Thread(target=self._thread_callback)
            self._consuming_thread.daemon = True
            self._consuming_thread.start()
