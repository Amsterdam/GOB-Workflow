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
from gobworkflow.workflow.start import start_workflows, start_step, has_no_errors


START = "start"  # workflow[START] is the name of the first step in a workflow

# The import workflow and steps
IMPORT = "import"
IMPORT_PREPARE = "prepare"
IMPORT_READ = "read"
IMPORT_COMPARE = "compare"
IMPORT_UPLOAD = "upload"
IMPORT_WORKFLOWS = "import_workflows"

# The export workflow and steps
EXPORT = "export"
EXPORT_GENERATE = "generate"
EXPORT_TEST = "test"

# The relate workflow and steps
RELATE = "relate"
RELATE_PARSE = "parse"
RELATE_WORKFLOWS = "relate_workflows"
RELATE_RELATION = "relate_relation"
RELATE_RELATE = "relate"
RELATE_COMPARE = "compare"
RELATE_UPLOAD = "upload"
RELATE_APPLY = "apply"

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
    IMPORT: {
        START: IMPORT_READ,
        IMPORT_PREPARE: {
            "function": lambda msg: start_step("prepare", msg),
            "next": [
                {
                    "step": IMPORT_WORKFLOWS
                }
            ]
        },
        IMPORT_WORKFLOWS: {
            "function": lambda msg: start_workflows(IMPORT, IMPORT_READ, msg)
        },
        IMPORT_READ: {
            "function": lambda msg: start_step("import", msg),
            "next": [
                {
                    "step": IMPORT_COMPARE
                }
            ],
        },
        IMPORT_COMPARE: {
            "function": lambda msg: start_step('compare', msg),
            "next": [
                {
                    "step": IMPORT_UPLOAD
                }
            ],
        },
        IMPORT_UPLOAD: {
            "function": lambda msg: start_step('fullupdate', msg)
        }
    },
    EXPORT: {
        START: EXPORT_GENERATE,
        EXPORT_GENERATE: {
            "function": lambda msg: start_step("export", msg)
        },
        EXPORT_TEST: {
            "function": lambda msg: start_step("export_test", msg)
        }
    },
    RELATE: {
        START: RELATE_PARSE,
        RELATE_PARSE: {
            "function": lambda msg: start_step("relate", msg),
            "next": [
                {
                    "step": RELATE_WORKFLOWS
                }
            ]
        },
        RELATE_WORKFLOWS: {
            "function": lambda msg: start_workflows(RELATE, RELATE_RELATE, msg)
        },
        RELATE_RELATE: {
            "function": lambda msg: start_step("relate_relation", msg),
            "next": [
                {
                    "step": RELATE_COMPARE
                }
            ]
        },
        RELATE_COMPARE: {
            "function": lambda msg: start_step('compare', msg),
            "next": [
                {
                    "step": RELATE_UPLOAD
                }
            ],
        },
        RELATE_UPLOAD: {
            "function": lambda msg: start_step('fullupdate', msg),
            "next": [
                {
                    "step": RELATE_APPLY
                }
            ],
        },
        RELATE_APPLY: {
            "function": lambda msg: start_step('apply_relation', msg)
        }
    }
}
