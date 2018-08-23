import sys
import pika

from gobworkflow.config import MESSAGE_BROKER, CONNECTION_PARAMS, QUEUES


def create_durable_message_queue(exchange, channel, name, route):
    """Creates a persistent message queue on the RabbitMQ message broker

    :param exchange: RabbitMQ exchande
    :param channel: RabbitMQ channel
    :param name: The name for queue to create
    :param route: The route(s) on the queue
    :return: RabbitMQ exchange
    """

    print(f"Create durable message queue {name} {route}")

    channel.queue_declare(
        queue=name,
        durable=True
    )

    channel.queue_bind(
        queue=name,
        exchange=exchange,
        routing_key=route
    )


if __name__ == "__main__":
    try:
        with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
            channel = connection.channel()

            for queue in QUEUES:
                channel.exchange_declare(
                    exchange=queue['exchange'],
                    exchange_type="topic",
                    durable=True)

                create_durable_message_queue(
                    exchange=queue['exchange'],
                    channel=channel,
                    name=queue['name'],
                    route=queue['key'])

            print(f"Succesfully created RabbitMQ message queues at '{MESSAGE_BROKER}'")

    except Exception as e:
        print(f"Error: Failed to connect to RabbitMQ at '{MESSAGE_BROKER}', {str(e)}")
        sys.exit(1)
