import os
import time
from urllib.error import URLError
import textwrap

import celery
import requests

import plaque_assay
import stitch_images


REDIS_PORT = 7777
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_NEUTRALISATION")


celery = celery.Celery(
    "task",
    backend=f"redis://localhost:{REDIS_PORT}/0",
    broker=f"redis://localhost:{REDIS_PORT}/0"
)


class BaseTask(celery.Task):

    @staticmethod
    def slack_alert(exc, task_id, args, kwargs, einfo):
        """send slack message on failure"""
        data = {
            "text": "Something broke",
            "username": "NE analysis",
            "attachments": [
                {
                    "text": textwrap.dedent(f"""
                        :fire: OH NO! :fire:
                        #####################################
                        {task_id!r} failed
                        #####################################
                        args: {args!r}
                        #####################################
                        {einfo!r}
                        #####################################
                        {exc!r}
                        #####################################
                    """),
                    "color": "#ad1721",
                    "attachment_type": "default",
                }
            ]
        }
        r = requests.post(SLACK_WEBHOOK_URL, json=data)
        return r.status_code

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """custom actions on task failure"""
        print("sending slack alert")
        status_code = self.slack_alert(exc, task_id, args, kwargs, einfo)
        if status_code != 200:
            print(f"{status_code}: failed to send slack alert")


@celery.task(
    queue="analysis",
    base=BaseTask,
    autoretry_for=(ConnectionResetError, FileNotFoundError, BlockingIOError)
)
def background_analysis_384(plate_list):
    """check for new experiment directory"""
    time.sleep(10)
    plaque_assay.main.run(plate_list)


@celery.task(
    queue="image_stitch",
    base=BaseTask,
    autoretry_for=(
        ConnectionResetError, FileNotFoundError, URLError, BlockingIOError
    )
)
def background_image_stitch_384(indexfile_path):
    """image stitching for 384 well plate"""
    time.sleep(10)
    stitcher = stitch_images.ImageStitcher(indexfile_path)
    stitcher.stitch_plate()
    stitcher.stitch_all_samples()
    stitcher.save_all()
