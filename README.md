# GOB-Workflow

GOB Workflow is the central component within GOB.

The following functions are performed:

## Routing

GOB Workflow will subscribe on all relevant queues and is responsable for routing received messages according to the defined workflows.

In its first version the workflow will:
- subscribe to log messages and store these messages in a database
- subscribe to workflow messages, inspect the message and route it further

## Monitoring

All log messages will be stored in a database and exposed over a REST API.
A frontend application can use this API to display the components, status and condition of the GOB components.

GOB components might publish heartbeats to be able to show if components are active or not.

## Control

Workflow message inspection might give rise to asking the user to confirm or reject the message.

An example might be that an update is received to delete 80% of all database rows of a specific table.
Instead of routing this message automatically to the next step and deleting the 80% records the user might be asked to confirm this.
The message will then be routed further or rejected.

