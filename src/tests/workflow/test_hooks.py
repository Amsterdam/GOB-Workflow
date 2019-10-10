from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

from gobworkflow.workflow.hooks import _get_hook_key, has_hooks, on_workflow_progress, handle_result
from gobworkflow.workflow.hooks import WORKFLOW_EXCHANGE, HOOK_KEY

class TestHooks(TestCase):

    def setUp(self) -> None:
        self.no_hook_msg = {}
        self.hook_key_value = 'Any hook key'
        self.hook_msg = {
            'header': {
                HOOK_KEY: self.hook_key_value
            }
        }

    def test_get_hook_key(self):
        key = _get_hook_key(self.no_hook_msg)
        self.assertIsNone(key)

        key = _get_hook_key(self.hook_msg)
        self.assertEqual(key, self.hook_key_value)

    def test_has_hooks(self):
        self.assertFalse(has_hooks(self.no_hook_msg))
        self.assertTrue(has_hooks(self.hook_msg))

    @patch('gobworkflow.workflow.hooks.publish')
    def test_on_workflow(self, mock_publish):
        on_workflow_progress(self.no_hook_msg)
        mock_publish.assert_not_called()

        on_workflow_progress(self.hook_msg)
        mock_publish.assert_called_with(WORKFLOW_EXCHANGE, self.hook_key_value, self.hook_msg)

    @patch('gobworkflow.workflow.hooks.publish')
    def test_handle_result(self, mock_publish):
        handle_result(self.no_hook_msg)
        mock_publish.assert_not_called()

        handle_result(self.hook_msg)
        mock_publish.assert_called_with(WORKFLOW_EXCHANGE, self.hook_key_value, self.hook_msg)