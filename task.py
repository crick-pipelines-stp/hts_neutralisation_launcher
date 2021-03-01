import time

from celery import Celery

import plaque_assay
import stitch_images


celery = Celery(
    "task", backend="redis://localhost:6379/0", broker="redis://localhost:6379/0"
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


@celery.task(queue="image_stitch")
def background_image_stitch_384(indexfile_path):
    """image stitching for 384 well plate"""
    time.sleep(10)
    stitcher = stitch_images.ImageStitcher(indexfile_path)
    stitcher.stitch_plate()
    stitcher.stitch_all_samples()
    stitcher.save_all()
