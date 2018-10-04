"""Message broker initialisation

The message broker serves a number of persistent queues holding persistent messages.

Creating persistent queues is an independent task.

The queues are required by the import and upload modules.
These modules are responsable for importing and uploading and not for creating the queues.

The initialisation of the queues is an integral part of the initialisation and startup of the message broker.

"""
import sys
import requests
import pika

from gobcore.message_broker.config import CONNECTION_PARAMS,\
                                          MESSAGE_BROKER, MESSAGE_BROKER_PORT, MESSAGE_BROKER_VHOST,\
                                          MESSAGE_BROKER_USER, MESSAGE_BROKER_PASSWORD,\
                                          WORKFLOW_EXCHANGE, LOG_EXCHANGE,\
                                          QUEUES


def _create_vhost(vhost):
    """
    Create a virtual host using the RabbtiMQ management interface

    :param vhost: name of the virtual host
    :return:
    """
    response = requests.put(
        url=f"http://{MESSAGE_BROKER}:{MESSAGE_BROKER_PORT}/api/vhosts/{vhost}",
        headers={
            "content-type": "application/json"
        },
        auth=(
            MESSAGE_BROKER_USER,
            MESSAGE_BROKER_PASSWORD
        ))
    response.raise_for_status()


def _create_exchange(channel, exchange, durable):
    """
    Create a RabbitMQ exchange

    :param channel: the RabbitMQ connection channel
    :param exchange: the name of the exchange
    :param durable: specifies wether the exchange should be persistent
    :return:
    """
    channel.exchange_declare(
        exchange=exchange,
        exchange_type="topic",
        durable=durable)


def _create_queue(channel, queue, durable):
    """
    Create a RabbitMQ queue

    :param channel: the RabbitMQ connection channel
    :param queue: the name of the queue
    :param durable: specifies wether the queue should be persistent
    :return:
    """
    channel.queue_declare(
        queue=queue,
        durable=durable
    )


def initialize_message_broker():
    """
    Initializes the RabbitMQ message broker.

    Creates a virtual host and persistent exxhanges and queues
    :return:
    """
    print(f"Initialize message broker {MESSAGE_BROKER}")

    print(f"Create virtual host {MESSAGE_BROKER_VHOST}")
    _create_vhost(MESSAGE_BROKER_VHOST)

    # Add exchanges and queues
    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:

        print("Connect to message broker")
        channel = connection.channel()

        for exchange in [WORKFLOW_EXCHANGE, LOG_EXCHANGE]:
            print(f"Create exchange {exchange}")
            _create_exchange(channel=channel, exchange=exchange, durable=True)

        for queue in QUEUES:
            print(f"Create queue {queue['name']}")
            _create_queue(channel=channel, queue=queue["name"], durable=True)


if __name__ == "__main__":
    try:
        initialize_message_broker()
    except Exception as e:
        print(f"Error: Failed to initialize message broker, {str(e)}")
        sys.exit(1)

    print("Succesfully initialized message broker")
