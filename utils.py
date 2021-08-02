import os
import textwrap
import requests


SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_NEUTRALISATION")


def is_even(n):
    return n % 2 == 0


def is_odd(n):
    return n % 2 != 0


def well_to_row_col(well):
    row = ord(well[0].lower()) - 96
    col = int(well[1:])
    return row, col


def get_dilution_from_row_col(row, col):
    row, col = int(row), int(col)
    if is_odd(row) and is_odd(col):
        dilution = 2560
    elif is_odd(row) and is_even(col):
        dilution = 160
    elif is_even(row) and is_odd(col):
        dilution = 640
    elif is_even(row) and is_even(col):
        dilution = 40
    else:
        raise RuntimeError()
    return dilution


def send_slack_alert(exc, task_id, args, kwargs, einfo):
    """send slack message on failure"""
    data = {
        "text": "Something broke",
        "username": "NE analysis",
        "attachments": [
            {
                "text": textwrap.dedent(
                    f"""
                    :fire: OH NO! :fire:
                    **NE pipeline**
                    #####################################
                    {task_id!r} failed
                    #####################################
                    args: {args!r}
                    #####################################
                    {einfo!r}
                    #####################################
                    {exc!r}
                    #####################################
                """
                ),
                "color": "#ad1720",
                "attachment_type": "default",
            }
        ],
    }
    r = requests.post(SLACK_WEBHOOK_URL, json=data)
    return r.status_code


def send_simple_slack_alert(workflow_id, variant, message):
    """send slack message on failure"""
    data = {
        "text": "Something broke",
        "username": "NE analysis",
        "attachments": [
            {
                "text": textwrap.dedent(
                    f"""
                    :fire: OH NO! :fire:
                    **neutralisation launcher**
                    Something went wrong with the launcher
                    #####################################
                    workflow_id: {workflow_id}
                    #####################################
                    variant: {variant}
                    #####################################
                    info:
                    {message}
                """
                ),
                "color": "#ad1720",
                "attachment_type": "default",
            }
        ],
    }
    r = requests.post(SLACK_WEBHOOK_URL, json=data)
    return r.status_code