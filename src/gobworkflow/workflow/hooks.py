from gobcore.message_broker import publish
from gobcore.message_broker.config import WORKFLOW_EXCHANGE

HOOK_KEY = "result_key"

def _get_hook_key(msg):
    return msg['header'].get(HOOK_KEY)


def has_hooks(msg):
    return _get_hook_key(msg) is not None


def on_workflow_progress(msg):
    key = _get_hook_key(msg)
    if not key:
        return
    publish(WORKFLOW_EXCHANGE, key, msg)


def handle_result(msg):
    key = _get_hook_key(msg)
    if not key:
        return
    publish(WORKFLOW_EXCHANGE, key, msg)
