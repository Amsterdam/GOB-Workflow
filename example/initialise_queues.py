import sys
import pika


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
        with pika.BlockingConnection(pika.ConnectionParameters('localhost')) as connection:
            channel = connection.channel()

            WORKFLOW = "gob.workflow"
            LOG = "gob.log"

            for exchange in [WORKFLOW, LOG]:
                channel.exchange_declare(
                    exchange=exchange,
                    exchange_type="topic",
                    durable=True)

            QUEUES = [
                (WORKFLOW, WORKFLOW + ".proposal", "*.proposal"),
                (WORKFLOW, WORKFLOW + ".request", "*.request"),
                (LOG, LOG + ".all", "#"),
            ]

            for exchange, name, route in QUEUES:
                create_durable_message_queue(
                    exchange=exchange,
                    channel=channel,
                    name=name,
                    route=route)

            print("Succesfully created RabbitMQ message queues at 'localhost'")

    except Exception as e:
        print("Error: Failed to connect to RabbitMQ at 'localhost', {str(e)}")
        sys.exit(1)
