import os
import textwrap
import requests
import logging


SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_NEUTRALISATION")
HOST_IP = "10.28.41.242"
PORT = 5555


def send_alert(exc, task_id, args, einfo):
    """send slack message on failure"""
    data = {
        "text": "Something broke",
        "username": "NE analysis",
        "attachments": [
            {
                "text": textwrap.dedent(
                    f"""
                    :fire: OH NO! :fire:
                    *NE pipeline*
                    -------------------------------------
                    {task_id!r} failed
                    http://{HOST_IP}:{PORT}/task/{task_id}
                    -------------------------------------
                    args: {args!r}
                    -------------------------------------
                    {einfo!r}
                    -------------------------------------
                    {exc!r}
                    -------------------------------------
                """
                ),
                "color": "#ad1720",
                "attachment_type": "default",
            }
        ],
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=data)
    log_response(response)


def send_warning(message):
    """send slack warning message"""
    data = {
        "text": "Something might be wrong",
        "username": "NE analysis",
        "attachments": [
            {
                "text": textwrap.dedent(
                    f"""
                    :warning: WARNING :warning:
                    *neutralisation launcher*
                    ----------------------------
                    {message}
                """
                ),
                "color": "#ffce00",
                "attachment_type": "default",
            }
        ],
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=data)
    log_response(response)


def send_simple_alert(workflow_id, variant, message):
    """send slack message on failure"""
    data = {
        "text": "Something broke",
        "username": "NE analysis",
        "attachments": [
            {
                "text": textwrap.dedent(
                    f"""
                    :fire: OH NO! :fire:
                    *neutralisation launcher*
                    Something went wrong with the launcher
                    -------------------------------------
                    workflow_id: {workflow_id}
                    -------------------------------------
                    variant: {variant}
                    -------------------------------------
                    info:
                    {message}
                    -------------------------------------
                """
                ),
                "color": "#ad1720",
                "attachment_type": "default",
            }
        ],
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=data)
    log_response(response)


def log_response(response):
    if response.status_code == 200:
        logging.info("message sent to slack")
    else:
        logging.error(f"failed to send slack message, code {response.status_code}")
