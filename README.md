# GOB-Workflow

GOB Workflow is the central component within GOB.

The following functions are performed:

## Routing

GOB Workflow will subscribe on all relevant queues and is responsable for routing received messages according to the defined workflows.

In its first version the workflow will:
- subscribe to log messages and print the messages on stdout
- subscribe to workflow messages, inspect the message and route it further

## Monitoring

All log messages will eventually be stored in a database and exposed over a REST API.
A frontend application can use this API to display the components, status and condition of the GOB components.

GOB components might publish heartbeats to be able to show if components are active or not.

## Control

Workflow message inspection might give rise to asking the user to confirm or reject the message.

An example might be that an update is received to delete 80% of all database rows of a specific table.
Instead of routing this message automatically to the next step and deleting the 80% records the user might be asked to confirm this.
The message will then be routed further or rejected.

# Requirements

    * docker-compose >= 1.17
    * docker ce >= 18.03
    * python >= 3.6
    
# Local Installation

Create a virtual environment:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r src/requirements.txt
    
Or activate the previously created virtual environment

    source venv/bin/activate
    
For the benefits of local development an instance of RabbitMQ can be spun up,
it will run on the gob-network, which needs to be created manually,
if it doesn't exist yet:

```bash
docker network create gob-network
```

Then start the dockerized instance of RabbitMQ:

```bash
docker-compose up rabbitmq &
```

This starts the RabbitMQ message broker with hostname "rabbitmq" on the gob-network.
Locally the RabbitMQ message broker can be accessed by "localhost".

RabbitMQ operates on port 5672 (used by Advanced Message Queuing Protocol (AMQP) 0-9-1 and 1.0 clients)

The [RabbitMQ management interface](https://www.rabbitmq.com/management.html) is available at port 15672:

    http://localhost:15672

If it is the first run of RabbitMQ, the GOB message queues need to be initialised 
_using the current virtual environment:_

```bash
(venv) $ python example/initialise_queues.py
```

# Running locally

Expose the IP address of the message broker in the environment:

```bash
export MESSAGE_BROKER_ADDRESS=localhost
```

Start the client, _using the virtual environment_:

    (venv) $ cd src
    (venv) $ python -m gobworkflow
    
## Tests

To run the tests, _using the virtual environment_:

    (venv) $ cd src
    (venv) $ sh test.sh

The coverage can be viewed by opening in the browser:

    htmlcov/index.html
