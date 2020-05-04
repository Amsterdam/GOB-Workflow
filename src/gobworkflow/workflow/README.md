# Workflow

## Tree
Workflows can be represented as tree structures. A tree structure allows for easy manipulation of workflows.  
Workflows can be changed or combined easily using the structure.

```tree.py``` contains the code to build such a tree.
Use that code as follows:

    from gobworkflow.workflow.config import WORKFLOWS, IMPORT
    tree = WorkflowTreeNode.from_dict(WORKFLOWS[IMPORT])

This creates the tree for the IMPORT workflow as defined in ```config.py```.
The resulting tree contains ```WorkflowTreeNode``` objects that represent the steps within the workflow. A
reference to the next step is encapsulated within a ```NextStep``` object. The ```NextStep``` object contains
a reference to the next ```WorkflowTreeNode``` along with the condition that needs to be met.

### Header parameters
The ```WorkflowTreeNode``` contains a dict property called ```header_parameters```. When this node is executed
the key/value pairs in ```header_parameters``` will be added to the message header that starts this step. An
example of this use is shown below in Dynamic Workflows.


## Dynamic Workflows
A dynamic workflow can be generated by passing a dynamic workflow definition to ```Workflow```.
For example:

    dynamic_workflow_steps = [
        {
            'type': 'workflow',
            'workflow': IMPORT,
            'header': {
                'catalogue': 'gebieden',
                'collection': 'stadsdelen',
                'application': 'DGDialog',
            }
        },
        {
            'type': 'workflow',
            'workflow': RELATE,
            'header': {
                'catalogue': 'gebieden',
                'collection': 'stadsdelen',
                'attribute': 'ligt_in_gemeente',
            }
        },    
    ] 

    workflow = Workflow('my_custom_workflow', dynamic_workflow_steps=dynamic_workflow_steps)

The resulting workflow can be used just like any other workflow (with ```start```, ```handle_result```, etc).
What happens is that first the IMPORT workflow is started with the given header attributes. The RELATE workflow
is then started when the last step of IMPORT has been completed successfully, without creating a new process. All
logging of both workflows are combined in one job of type 'my_custom_workflow'.
Just like any other workflow we can also provide a step_name to the Workflow constructor.

For completeness, this is how we would create a dynamic workflow by sending a message gobworkflow:

    connection.publish(WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY, {
            'header': {
                'workflow': [
                    {
                        'type': 'workflow',
                        'workflow': IMPORT,
                        'header': {
                            'catalogue': 'gebieden',
                            'collection': 'stadsdelen',
                            'application': 'DGDialog',
                        }
                    },
                    {
                        'type': 'workflow',
                        'workflow': RELATE,
                        'header': {
                            'catalogue': 'gebieden',
                            'collection': 'stadsdelen',
                            'attribute': 'ligt_in_gemeente',
                        }
                    },
                ]
            },
            'workflow': {
                'workflow_name': 'my_custom_test',
            }
        })

We send the dynamic workflow in the header under the ```workflow``` key, and provide a workflow name  
in the body just as we start any other workflow, except this workflow name can be anything we find suitable  
to identify this dynamic workflow.

### Dynamic workflow steps
The dynamic workflow definition also allows for adding dynamic steps. These look as follows:

    {
        'type': 'workflow_step',
        'step_name': 'my_step_name,
        'header': {
            ...    
        }
    },

When the dynamic workflow builder finds a type 'workflow_step', it uses the ```start_step``` function to
send a message to the workflow exchange with 'my_step_name' as key. Workflow does not care about who takes this
message; it is on the implementing side to make sure there is some queue listening to 'my_step_name'.