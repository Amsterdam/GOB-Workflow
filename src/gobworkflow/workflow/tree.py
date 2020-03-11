"""Contains classes that represent a Workflow as a tree.

A tree is build by simply passing the workflow and optionally a step name.

Example, this builds a tree for the IMPORT workflow:

from gobworkflow.workflow.config import WORKFLOWS, IMPORT

tree = WorkflowTreeNode.from_dict(WORKFLOWS[IMPORT])


"""
from gobworkflow.workflow.config import START, DEFAULT_CONDITION
from typing import List, Callable


class WorkflowTreeNode:
    """Class representing a Workflow (sub)tree."""

    def __init__(self, name, function=None, next=None):
        """Don't use directly. Use from_dict instead.


        :param name:
        :param function:
        :param next:
        """
        self.name = name
        self.function = function or (lambda _: None)
        self.next = next or []

    @staticmethod
    def from_dict(workflow: dict, step_name=START) -> 'WorkflowTreeNode':
        """
        Usage (WORKFLOWS[IMPORT] refers to the config in gobworkflow.workflow):

        tree = WorkflowTreeNode.from_dict(WORKFLOWS[IMPORT])

        :param workflow:
        :param step_name:
        :return:
        """
        step = workflow[step_name]

        if isinstance(step, str):
            # Step is a reference to another step
            return WorkflowTreeNode.from_dict(workflow, step)

        step = workflow[step_name]

        return WorkflowTreeNode(
            step_name,
            step.get('function'),
            [NextStep.from_dict(workflow, next) for next in step.get('next', [])]
        )

    def to_dict(self):
        """Returns dict representation of this tree.
        Note: This representation differs from the config dict, as the next steps are inlined instead of referenced.

        :return:
        """
        return {
            self.name: {
                'function': self.function,
                'next': [n.to_dict() for n in self.next]
            }
        }

    def get_leafs(self) -> List['WorkflowTreeNode']:
        """Returns all leaf nodes in this tree

        :return:
        """
        if not self.next:
            return [self]

        return [leaf for n in self.next for leaf in n.node.get_leafs()]

    def get_node(self, name):
        """Returns node by name

        :param name:
        :return:
        """
        if self.name == name:
            return self

        for n in self.next:
            node = n.node.get_node(name)

            if node:
                return node
        return None

    def append_node(self, node: 'WorkflowTreeNode', condition=None):
        """Appends node to this node

        :param node:
        :param condition:
        :return:
        """
        self.next.append(NextStep(node, condition))

    def append_to_names(self, append_str: str):
        """Appends append_str to all node names in this tree.
        Useful when merging multiple trees cause name collisions.

        :param append_str:
        :return:
        """
        self.name = f'{self.name}_{append_str}'

        for n in self.next:
            n.node.append_to_names(append_str)


class NextStep:
    """Wraps a next step in a workflow tree with its condition.

    """

    def __init__(self, node: WorkflowTreeNode, condition: Callable = None):
        self.node = node
        self.condition = condition or DEFAULT_CONDITION

    @staticmethod
    def from_dict(workflow: dict, next: dict) -> 'NextStep':
        return NextStep(WorkflowTreeNode.from_dict(workflow, next['step']), next.get('condition'))

    def to_dict(self) -> dict:
        return {
            'step': self.node.to_dict(),
            'condition': self.condition,
        }
