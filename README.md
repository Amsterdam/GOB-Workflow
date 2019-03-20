# GOB-Workflow

GOB Workflow is the central component within GOB.

GOB Workflow subscribes to all relevant queues
and is responsable for routing received messages according to the defined workflows:
- subscribe to log messages and store the messages in the management database
- subscribe to workflow messages, inspect the message and route it further

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Run

```bash
docker-compose build
docker-compose up &
```

## Tests

```bash
docker-compose -f src/.jenkins/test/docker-compose.yml build
docker-compose -f src/.jenkins/test/docker-compose.yml run test
```

# Local

## Requirements

* python >= 3.6
    
## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```
    
Or activate the previously created virtual environment

```bash
source venv/bin/activate
```
    
# Run

Start the service:

```bash
cd src
python -m gobworkflow
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```
