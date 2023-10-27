import logging
import os
import textwrap

import requests

from .config import parse_config

log = logging.getLogger(__name__)
cfg = parse_config()
HOST_IP = cfg["default"]["host_ip"]
PORT = cfg["celery"]["flower_port"]

RED = "#ad1720"
YELLOW = "#ffce00"

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_NEUTRALISATION")
if not SLACK_WEBHOOK_URL:
    raise EnvironmentError(
        "'SLACK_WEBHOOK_NEUTRALISATION' environment variable not found.",
        "A slack webhook url is required to send error notifications.",
    )


def send_alert(exc, task_id, args, kwargs, einfo) -> None:
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
                "color": RED,
                "attachment_type": "default",
            }
        ],
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=data)
    log_response(response)


def send_warning(message: str) -> None:
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
                "color": YELLOW,
                "attachment_type": "default",
            }
        ],
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=data)
    log_response(response)


def send_simple_alert(workflow_id: str | int, variant: str, message: str) -> None:
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
                "color": RED,
                "attachment_type": "default",
            }
        ],
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=data)
    log_response(response)


def log_response(response: requests.Response) -> None:
    if response.status_code == 200:
        log.info("message sent to slack")
    else:
        log.error(f"failed to send slack message, code {response.status_code}")
