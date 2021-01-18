import time

from celery import Celery

import plaque_assay


celery = Celery(
    "task",
    backend="redis://redis/0",
    broker="redis://redis/0",
)


@celery.task(queue="analysis")
def background_analysis(plate_list):
    """check for new experiment directory"""
    output_dir = "/usr/src/app/test_output"
    time.sleep(10)
    plaque_assay.main.run(plate_list, output_dir)
