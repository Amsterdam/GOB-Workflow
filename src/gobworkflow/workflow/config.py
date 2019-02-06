"""
Workflow configuration

Multiple workflows can be defined
Each workflow has a name and one or more workflow steps

Every step in a workflow has a function to be executed when the step is started
When a step has finished a next step is searched

Multiple next steps may exist for a workflow step
Each next step may define a condition that needs to be met in order to get eligible for execution

When one or more next steps match its condition, the first one will be executed
If no next steps are defined on can be found the workflow is ended
"""
from gobcore.message_broker.config import IMPORT_QUEUE, EXPORT_QUEUE, REQUEST_QUEUE
from gobcore.message_broker import publish


START = "start"  # workflow[START] is the name of the first step in a workflow

# The import workflow and steps
IMPORT = "import"
IMPORT_READ = "read"
IMPORT_COMPARE = "compare"
IMPORT_UPLOAD = "upload"

# The export workflow and steps
EXPORT = "export"
EXPORT_GENERATE = "generate"

# The relate workflow and steps
RELATE = "relate"
RELATE_RELATE = "relate"

# The GOB workflows
WORKFLOWS = {
    IMPORT: {
        START: IMPORT_READ,
        IMPORT_READ: {
            "function": lambda msg: publish(IMPORT_QUEUE, "import.start", msg),  # default: "function": lambda _: None
            "next": [  # default: "next": []
                {
                    # default: "condition": lambda: True
                    "step": IMPORT_COMPARE
                }
            ],
        },
        IMPORT_COMPARE: {
            "function": lambda msg: publish(REQUEST_QUEUE, 'fullimport.request', msg),
            "next": [
                {
                    "step": IMPORT_UPLOAD
                }
            ],
        },
        IMPORT_UPLOAD: {
            "function": lambda msg: publish(REQUEST_QUEUE, 'fullupdate.request', msg)
        }
    },
    EXPORT: {
        START: EXPORT_GENERATE,
        EXPORT_GENERATE: {
            "function": lambda msg: publish(EXPORT_QUEUE, "export.start", msg)
        }
    },
    RELATE: {
        START: RELATE_RELATE,
        RELATE_RELATE: {
            "function": lambda msg: publish(REQUEST_QUEUE, "fullrelate.request", msg)
        }
    }
}
