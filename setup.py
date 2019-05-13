import os
from setuptools import setup, find_packages


abs_path = os.path.dirname(__file__)


# def read(file_name):
#     return open(os.path.join(abs_path, file_name)).read()


setup(
    name='zayats',
    version='0.2.0',
    packages=find_packages(),
    url='https://github.com/RusJr/zayats',
    license='MIT',
    author='Rus Jr',
    author_email='binderrrr@gmail.com',
    keywords='pika rabbit rabbitmq',
    description='RabbitMQ simple fault-tolerant client (pika wrapper)',

    python_requires='>=3.5',
    install_requires=['pika==1.0.1']
)
