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
from gobworkflow.workflow.start import start_step, has_no_errors
from gobcore.exceptions import GOBException
from gobcore.message_broker.config import APPLY, COMPARE, FULLUPDATE, PREPARE, BAG_EXTRACT, \
    RELATE_PREPARE, RELATE_PROCESS, RELATE_CHECK, RELATE_UPDATE_VIEW,\
    EXPORT, EXPORT_TEST, END_TO_END_TEST, DATA_CONSISTENCY_TEST, BRP_REGRESSION_TEST,\
    DISTRIBUTE, KAFKA_PRODUCE

START = "start"  # workflow[START] is the name of the first step in a workflow

# The prepare workflow and steps
PREPARE_START = "prepare_start"

# The bag extract workflow
BAG_EXTRACT_START = "bag_extract_start"

# The import workflow and steps
IMPORT = "import"
IMPORT_OBJECT = "import_object"
IMPORT_READ = "read"
UPDATE_MODEL = "update_model"
APPLY_EVENTS = "apply_events"
IMPORT_COMPARE = "compare"
IMPORT_UPLOAD = "upload"
UPLOAD_RELATION = "upload_relation"

# The export workflow and steps
EXPORT_GENERATE = "generate"

# The relate workflow
RELATE = "relate"

END_TO_END_TEST_START = "end_to_end_test_start"
DATA_CONSISTENCY_TEST_START = "data_consistency_test_start"
BRP_REGRESSION_TEST_START = "brp_regression_test_start"
DISTRIBUTE_START = "distribute_start"

# Default check for absence of errors before starting next step
DEFAULT_CONDITION = has_no_errors

# The GOB workflows
WORKFLOWS = {
    # Example
    # WORKFLOW_NAME: {
    #     START: STEP_NAME,
    #     STEP_NAME: {
    #         "function": lambda _: None,  # default value
    #         "next": [  # default: "next": []
    #             {
    #                 "condition": DEFAULT_CONDITION,  # default value
    #                 "step": ANOTHER_STEP_NAME  # required
    #             }
    #         ],
    #     }
    # },
    UPDATE_MODEL: {
        START: UPDATE_MODEL,
        UPDATE_MODEL: {
            "function": lambda msg: start_step(APPLY, msg)
        },
    },
    PREPARE: {
        START: PREPARE_START,
        PREPARE_START: {
            "function": lambda msg: start_step(PREPARE, msg),
        }
    },
    BAG_EXTRACT: {
        START: BAG_EXTRACT_START,
        BAG_EXTRACT_START: {
            "function": lambda msg: start_step(BAG_EXTRACT, msg),
        }
    },
    IMPORT: {
        START: IMPORT_READ,
        IMPORT_READ: {
            "function": lambda msg: start_step(IMPORT, msg),
            "next": [
                {
                    "step": UPDATE_MODEL
                }
            ],
        },
        UPDATE_MODEL: {
            "function": lambda msg: start_step(APPLY, {
                **msg,
                "header": {
                    **msg["header"],
                    "suppress_notifications": True,
                }
            }),
            "next": [
                {
                    "step": IMPORT_COMPARE
                }
            ],
        },
        IMPORT_COMPARE: {
            "function": lambda msg: start_step(COMPARE, {
                **msg,
                "header": {
                    **msg["header"],
                    "suppress_notifications": False,
                }
            }),
            "next": [
                {
                    "step": IMPORT_UPLOAD
                }
            ],
        },
        IMPORT_UPLOAD: {
            "function": lambda msg: start_step(FULLUPDATE, msg),
            "next": [
                {
                    "step": APPLY_EVENTS
                }
            ],
        },
        APPLY_EVENTS: {
            "function": lambda msg: start_step(APPLY, msg)
        },
    },
    IMPORT_OBJECT: {
        START: IMPORT_OBJECT,
        IMPORT_OBJECT: {
            "function": lambda msg: start_step(IMPORT_OBJECT, msg),
            "next": [
                {
                    # Continue in import workflow
                    "workflow": IMPORT,
                    "step": UPDATE_MODEL,
                }
            ]
        },
    },
    EXPORT: {
        START: EXPORT_GENERATE,
        EXPORT_GENERATE: {
            "function": lambda msg: start_step(EXPORT, msg)
        },
    },
    EXPORT_TEST: {
        START: EXPORT_TEST,
        EXPORT_TEST: {
            "function": lambda msg: start_step(EXPORT_TEST, msg)
        },
    },
    RELATE: {
        START: RELATE_PREPARE,
        RELATE_PREPARE: {
            "function": lambda msg: start_step(RELATE_PREPARE, msg),
            "next": [
                {
                    "step": RELATE_PROCESS,
                    "condition": lambda msg: not msg.get('header', {}).get('is_split', False),
                }
            ]
        },
        RELATE_PROCESS: {
            "function": lambda msg: start_step(RELATE_PROCESS, msg),
            "next": [
                {
                    "step": IMPORT_UPLOAD,
                }
            ]
        },
        IMPORT_UPLOAD: {
            "function": lambda msg: start_step(FULLUPDATE, msg),
            "next": [
                {
                    "step": APPLY_EVENTS
                }
            ],
        },
        APPLY_EVENTS: {
            "function": lambda msg: start_step(APPLY, msg),
            "next": [
                {
                    "condition": lambda _: True,
                    "step": RELATE_UPDATE_VIEW,
                }
            ]
        },
        RELATE_UPDATE_VIEW: {
            "function": lambda msg: start_step(RELATE_UPDATE_VIEW, msg),
            "next": [
                {
                    "condition": lambda _: True,
                    "step": RELATE_CHECK
                }
            ]
        },
        RELATE_CHECK: {
            "function": lambda msg: start_step(RELATE_CHECK, msg)
        }
    },
    END_TO_END_TEST: {
        START: END_TO_END_TEST_START,
        END_TO_END_TEST_START: {
            "function": lambda msg: start_step(END_TO_END_TEST, msg)
        }
    },
    DATA_CONSISTENCY_TEST: {
        START: DATA_CONSISTENCY_TEST_START,
        DATA_CONSISTENCY_TEST_START: {
            "function": lambda msg: start_step(DATA_CONSISTENCY_TEST, msg)
        }
    },
    BRP_REGRESSION_TEST: {
        START: BRP_REGRESSION_TEST_START,
        BRP_REGRESSION_TEST_START: {
            "function": lambda msg: start_step(BRP_REGRESSION_TEST, msg),
        },
    },
    DISTRIBUTE: {
        START: DISTRIBUTE_START,
        DISTRIBUTE_START: {
            "function": lambda msg: start_step(DISTRIBUTE, msg),
        },
    },
    KAFKA_PRODUCE: {
        START: 'kafka_start',
        'kafka_start': {
            'function': lambda msg: start_step(KAFKA_PRODUCE, msg),
        }
    }
}


def get_workflow(workflow: str):
    if workflow not in WORKFLOWS:
        raise GOBException(f"Workflow '{workflow}' is not defined")
    return WORKFLOWS[workflow]
