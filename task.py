import time

from celery import Celery

import plaque_assay


celery = Celery(
    "task",
    backend="redis://localhost:6379/0",
    broker="redis://localhost:6379/0"
)


@celery.task(queue="analysis")
def background_analysis_96(plate_list):
    """check for new experiment directory"""
    time.sleep(10)
    plaque_assay.main.run(plate_list, plate=96)


@celery.task(queue="analysis")
def background_analysis_384(plate_list):
    """check for new experiment directory"""
    time.sleep(10)
    plaque_assay.main.run(plate_list, plate=384)
