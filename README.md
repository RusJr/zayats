# Zayats

## Description
RabbitMQ simple fault-tolerant connector (pika based). 
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
    publisher.publish(queue_name=TEST_QUEUE, data={'Hello': 'world'})

    # consuming loop ---------------------------------------
    consumer = RabbitConsumer(PIKA_PARAMS, queue=TEST_QUEUE)
    while True:
        message = consumer.send_ack_and_get_new_msg(timeout=10)

```
