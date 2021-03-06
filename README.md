# Zayats

## Description
RabbitMQ simple fault-tolerant client (pika wrapper). 
Supports JSON message format only.

## Installition
pip install zayats

## Example

```python
import pika
from zayats import RabbitPublisher, RabbitConsumer


TEST_QUEUE = 'test_queue'
PIKA_PARAMS = pika.ConnectionParameters(
    host='0.0.0.0', port=2001, heartbeat=0, credentials=pika.credentials.PlainCredentials('rabbit', '123'),
)


if __name__ == '__main__':

    # publishing example --------------------------------------------
    publisher = RabbitPublisher(PIKA_PARAMS)
    publisher.pika_channel.queue_declare(queue=TEST_QUEUE, durable=True)
    publisher.publish(queue_name=TEST_QUEUE, data={'Hello': 'world'}, declare_queue=False)

    # consuming loop ---------------------------------------
    consumer = RabbitConsumer(PIKA_PARAMS, queue=TEST_QUEUE)
    while True:
        message = consumer.send_ack_and_get_new_msg(timeout=10)

```
