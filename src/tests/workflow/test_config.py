from unittest import TestCase
from unittest.mock import patch

from gobworkflow.workflow.config import get_workflow, GOBException


class TestModuleFunctions(TestCase):

    def test_get_workflow(self):
        workflows = {
            'workflow a': 'WF A',
            'workflow b': 'WF B',
        }
        with patch("gobworkflow.workflow.config.WORKFLOWS", workflows):
            self.assertEqual('WF B', get_workflow('workflow b'))

            with self.assertRaises(GOBException):
                get_workflow('workflow c')
