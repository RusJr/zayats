import json
import logging.config
from _socket import gaierror
from time import sleep

import pika
from pika.exceptions import ConnectionClosed, IncompatibleProtocolError
from queue import Queue, Empty
from threading import Thread


class RabbitConsumer:

    _logger = logging.getLogger('RabbitConsumer')
    acknowledge_period = 10  # seconds
    check_connection_period = 10  # seconds
    reconnect_sleep = 10  # seconds

    def __init__(self, pika_params: pika.ConnectionParameters, queue: str, exchange: str = '', exchange_type: str = '',
                 lazy_connection=True):
        self._pika_params = pika_params
        self._pika_queue = queue
        self.exchange = exchange
        self.exchange_type = exchange_type

        self._pika_connection = None
        self._pika_channel = None
        if not lazy_connection:
            self._check_connection()

        self._current_task = None
        self._python_q_task = Queue()
        self._python_q_acknowledge = Queue()

        self._thread = None

    def __del__(self):
        if hasattr(self, '_pika_connection') and self._pika_connection and not self._pika_connection.is_closed:
            self._pika_connection.close()

    def send_ack(self, stop_consuming=True):
        if self._current_task is not None:
            self._python_q_acknowledge.put('gotovo yopta')
            self._current_task = None
            if stop_consuming:
                self._pika_connection.close()
                self._logger.debug('[send_ack] Connection closed')

    def send_ack_and_get_new_msg(self, timeout=None):
        self._check_thread()
        self.send_ack(stop_consuming=False)
        _timeout = min(self.check_connection_period, timeout) if timeout else self.check_connection_period
        _total_spent_time = 0
        while not timeout or _total_spent_time < timeout:
            try:
                self._current_task = self._python_q_task.get(timeout=_timeout)
            except Empty:
                self._check_thread()
                if timeout:
                    _total_spent_time += _timeout
            else:
                return self._current_task
        self._logger.debug('No messages (timeout)')
        return None

    def _check_connection(self):
        while not self._pika_connection or self._pika_connection.is_closed:
            try:
                self._pika_connection = pika.BlockingConnection(parameters=self._pika_params)
                self._pika_channel = self._pika_connection.channel()
                self._pika_channel.basic_qos(prefetch_count=1)
                self._pika_channel.queue_declare(queue=self._pika_queue, durable=True)
                if self.exchange:
                    self._pika_channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
                    self._pika_channel.queue_bind(exchange=self.exchange, queue=self._pika_queue)
            except (ConnectionClosed, IncompatibleProtocolError, gaierror) as e:
                self._logger.critical('Rabbit connection problem. Retry after %d seconds (%s)', self.reconnect_sleep, e)
                sleep(self.reconnect_sleep)
            else:
                self._logger.info('Connected with RabbitMQ')

    def _wait(self):
        while True:
            try:
                self._python_q_acknowledge.get(timeout=self.acknowledge_period)  # waiting for ack order
            except Empty:
                self._pika_connection.process_data_events()
            else:
                return

    def _consume_func(self, ch, method, properties, body):
        try:
            received_task = json.loads(body.decode())
        except json.JSONDecodeError:
            self._logger.error('[RabbitConsumer] Task skipped. JSONDecodeError on "%s"', body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self._python_q_task.put(received_task)
            self._logger.debug('Got msg: %s', received_task)
            self._wait()
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def _check_thread(self):
        if not self._thread or not self._thread.is_alive():
            self._check_connection()
            self._pika_channel.basic_consume(self._consume_func, queue=self._pika_queue, no_ack=False)
            self._thread = Thread(target=self._pika_channel.start_consuming)
            self._thread.daemon = True
            self._thread.start()
            self._logger.debug('Consuming thread has been started')
