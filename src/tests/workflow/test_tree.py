from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobworkflow.workflow.tree import WorkflowTreeNode, NextStep, START


class TestWorkflowTreeNode(TestCase):

    def test_init(self):
        wf = WorkflowTreeNode('some name')
        self.assertEqual('some name', wf.name)
        self.assertEqual([], wf.next)
        self.assertIsNone(wf.function('some argument'))

        wf = WorkflowTreeNode('some other name', lambda x: x*2, ['next'])
        self.assertEqual('some other name', wf.name)
        self.assertEqual('aa', wf.function('a'))
        self.assertEqual(['next'], wf.next)

    @patch("gobworkflow.workflow.tree.NextStep")
    def test_from_dict(self, mock_next_step):
        workflow = {
            START: 'step1',
            'step1': {
                'function': 'the function',
                'next': [{'step': 'stuff'}]
            },
        }

        result = WorkflowTreeNode.from_dict(workflow)
        self.assertEqual('step1', result.name)
        self.assertEqual('the function', result.function)
        self.assertEqual([mock_next_step.from_dict.return_value], result.next)

        mock_next_step.from_dict.assert_called_with(workflow, {'step': 'stuff'})

        # Should yield same result
        result2 = WorkflowTreeNode.from_dict(workflow, 'step1')
        self.assertEqual(result.name, result2.name)
        self.assertEqual(result.function, result2.function)
        self.assertEqual(result.next, result2.next)

    @patch("gobworkflow.workflow.tree.get_workflow")
    @patch("gobworkflow.workflow.tree.NextStep")
    def test_from_dict_jumping_workflows(self, mock_next_step, mock_get_workflow):
        workflow = {
            START: 'step1',
            'step1': {
                'function': 'the function',
                'next': [{
                    'workflow': 'other_workflow',
                    'step': 'stuff'
                }]
            },
        }

        result = WorkflowTreeNode.from_dict(workflow)
        self.assertEqual('step1', result.name)
        self.assertEqual('the function', result.function)
        self.assertEqual([mock_next_step.from_dict.return_value], result.next)

        mock_next_step.from_dict.assert_called_with(mock_get_workflow.return_value, {'workflow': 'other_workflow', 'step': 'stuff'})
        mock_get_workflow.assert_called_with('other_workflow')

    def test_to_dict(self):
        wf = WorkflowTreeNode('the name', 'the function', [
            type('', (), {'to_dict': lambda: 'next 1'}),
            type('', (), {'to_dict': lambda: 'next 2'}),
        ])

        self.assertEqual({
            'the name': {
                'function': 'the function',
                'next': ['next 1', 'next 2']
            }
        }, wf.to_dict())

    def test_get_leafs(self):
        wf = WorkflowTreeNode('')
        self.assertEqual([wf], wf.get_leafs())

        next1 = MagicMock()
        next2 = MagicMock()
        next1.node.get_leafs.return_value = ['a', 'b']
        next2.node.get_leafs.return_value = ['d', 'e']

        wf = WorkflowTreeNode('', next=[next1, next2])
        self.assertEqual(['a', 'b', 'd', 'e'], wf.get_leafs())

    def test_get_node(self):
        wf = WorkflowTreeNode('my name')
        self.assertEqual(wf, wf.get_node('my name'))

        next1 = MagicMock()
        next1.node.get_node.return_value = None
        next2 = MagicMock()
        next2.node.get_node.return_value = next2
        wf.next = [next1, next2]

        self.assertEqual(next2, wf.get_node('your name'))
        next1.node.get_node.assert_called_with('your name')
        next2.node.get_node.assert_called_with('your name')

        next2.node.get_node.return_value = None
        self.assertIsNone(wf.get_node('your name'))

    @patch("gobworkflow.workflow.tree.NextStep")
    def test_append_node(self, mock_next_step):
        wf = WorkflowTreeNode('node 1')
        wf2 = WorkflowTreeNode('node 2')

        wf.append_node(wf2, 'some condition')
        self.assertEqual(wf.next, [mock_next_step.return_value])
        mock_next_step.assert_called_with(wf2, 'some condition')

    def test_append_to_names(self):
        wf = WorkflowTreeNode('node')
        wf.next = [MagicMock()]

        wf.append_to_names('appended')
        self.assertEqual('node_appended', wf.name)
        wf.next[0].node.append_to_names.assert_called_with('appended')

    def test_set_header_parameters(self):
        wf = WorkflowTreeNode('')
        self.assertEqual({}, wf.header_parameters)

        wf.set_header_parameters({'some': 'parameters'})
        self.assertEqual({'some': 'parameters'}, wf.header_parameters)

    def test_to_string(self):
        next1 = MagicMock()
        next1.node._to_string.return_value = '    child1\n'
        next2 = MagicMock()
        next2.node._to_string.return_value = '    child2\n'

        wf = WorkflowTreeNode('name')
        wf.header_parameters = {'k1': 'v1', 'k2': 'v2'}
        wf.next = [next1, next2]

        expected = '  name (k1:v1, k2:v2)\n    child1\n    child2\n'
        self.assertEqual(expected, wf._to_string(1))
        next1.node._to_string.assert_called_with(2)
        next2.node._to_string.assert_called_with(2)

    def test_str(self):
        wf = WorkflowTreeNode('')
        wf._to_string = MagicMock(return_value='str repr')
        self.assertEqual('str repr', str(wf))
        wf._to_string.assert_called_with()


class TestNextStep(TestCase):

    @patch("gobworkflow.workflow.tree.DEFAULT_CONDITION", lambda: 'default condition')
    def test_init(self):
        node = MagicMock()
        ns = NextStep(node)
        self.assertEqual(ns.node, node)
        self.assertEqual(ns.condition(), 'default condition')

        ns = NextStep(node, lambda: 'custom condition')
        self.assertEqual(ns.node, node)
        self.assertEqual(ns.condition(), 'custom condition')

    @patch("gobworkflow.workflow.tree.WorkflowTreeNode")
    def test_from_dict(self, mock_tree):
        workflow = {'some': 'workflow'}
        next = {'step': 'the next step', 'condition': 'the condition'}

        result = NextStep.from_dict(workflow, next)
        self.assertEqual(mock_tree.from_dict.return_value, result.node)
        self.assertEqual('the condition', result.condition)

        mock_tree.from_dict.assert_called_with(workflow, 'the next step')

    def test_to_dict(self):
        node = MagicMock()
        condition = MagicMock()
        ns = NextStep(node, condition)

        self.assertEqual({
            'step': node.to_dict.return_value,
            'condition': condition
        }, ns.to_dict())
