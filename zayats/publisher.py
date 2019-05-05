import json
import logging.config
from time import sleep
from typing import Union

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError

from zayats.utils import set_logger


class RabbitPublisher:

    reconnect_sleep = 10  # seconds

    def __init__(self, pika_params: pika.ConnectionParameters, lazy_connection=True, reconnect_sleep=reconnect_sleep,
                 logging_level='INFO'):

        # logging ------------------------------------
        _logger_name = type(self).__name__
        set_logger(_logger_name, logging_level)
        self._logger = logging.getLogger(_logger_name)

        self.reconnect_sleep = reconnect_sleep

        self._pika_params = pika_params
        self._pika_connection = None
        self._pika_channel = None
        if not lazy_connection:
            self._open_connection()

    def __del__(self):
        if hasattr(self, '_pika_connection') and self._pika_connection and not self._pika_connection.is_closed:
            self._pika_connection.close()

    @property
    def pika_connection(self) -> pika.BlockingConnection:
        self._check_connection()
        return self._pika_connection

    @property
    def pika_channel(self) -> BlockingChannel:
        self._check_connection()
        return self._pika_channel

    def publish(self, queue_name: str, data: Union[dict, list, str, int, bool], reconnect=False):

        try:
            self._publish(data, queue_name)
        except AMQPConnectionError:
            self._check_connection(retry=reconnect)
            self._publish(data, queue_name)
        self._logger.debug('Published: %s', data)

    def _no_connection(self) -> bool:
        return not self._pika_connection or self._pika_connection.is_closed

    def _check_connection(self, retry=False):
        if retry:
            while self._no_connection():
                try:
                    self._open_connection()
                except AMQPConnectionError as e:
                    self._logger.critical('%s(%s). Retry after %d seconds', type(e).__name__, e, self.reconnect_sleep)
                    sleep(self.reconnect_sleep)
        else:
            if self._no_connection():
                self._open_connection()

    def _open_connection(self):
        self._pika_connection = pika.BlockingConnection(parameters=self._pika_params)
        self._pika_channel = self._pika_connection.channel()
        self._pika_channel.basic_qos(prefetch_count=1)
        self._logger.info('Connected with RabbitMQ(%s:%s)', self._pika_params.host, self._pika_params.port)

    def _publish(self, data: dict, queue_name=None):
        """
        :raise pika.exceptions.ConnectionClosed
        :param data:
        :param queue_name:
        :return: None
        """
        self.pika_channel.queue_declare(queue=queue_name, durable=True)
        self.pika_channel.basic_publish(exchange='',
                                        routing_key=queue_name,
                                        body=json.dumps(data, ensure_ascii=False),
                                        properties=pika.BasicProperties(delivery_mode=2))
