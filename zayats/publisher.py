import json
import logging.config
from typing import Union

import pika
from pika.exceptions import ConnectionClosed, IncompatibleProtocolError


class RabbitPublisher:

    _logger = logging.getLogger('RabbitPublisher')

    def __init__(self, pika_params: pika.ConnectionParameters, lazy_connection=True):
        self._pika_params = pika_params
        self._pika_connection = None
        self._pika_channel = None
        if not lazy_connection:
            self._open_connection()

    def __del__(self):
        if hasattr(self, '_pika_connection') and self._pika_connection and not self._pika_connection.is_closed:
            self._pika_connection.close()

    def publish(self, queue_name: str, data: Union[dict, list, str, int, bool]):
        try:
            if not self._pika_connection or self._pika_connection.is_closed:
                raise ConnectionClosed
            self._publish(data, queue_name)
        except (ConnectionClosed, IncompatibleProtocolError):
            self._open_connection()
            self._publish(data, queue_name)
        self._logger.debug('Published: %s', data)

    def _open_connection(self):
        self._pika_connection = pika.BlockingConnection(parameters=self._pika_params)
        self._pika_channel = self._pika_connection.channel()
        self._pika_channel.basic_qos(prefetch_count=1)
        self._logger.info('Connected with RabbitMQ')

    def _publish(self, data: dict, queue_name=None):
        """
        :raise pika.exceptions.ConnectionClosed
        :param data:
        :param queue_name:
        :return: None
        """
        self._pika_channel.queue_declare(queue=queue_name, durable=True)
        self._pika_channel.basic_publish(exchange='',
                                         routing_key=queue_name,
                                         body=json.dumps(data, ensure_ascii=False),
                                         properties=pika.BasicProperties(delivery_mode=2))
