from prefect import task
import requests


def send_notification(obj, old_state, new_state):
    """Sends a POST request with the error message if task fails"""
    if new_state.is_failed():
        requests.post("http://example.com", json={"error_msg": str(new_state.result)})
    return new_state


@task(state_handlers=[send_notification])
def do_nothing():
    pass
