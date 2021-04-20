import time
from urllib.error import URLError

from celery import Celery

import plaque_assay
import stitch_images


REDIS_PORT = 7777

celery = Celery(
    "task",
    backend=f"redis://localhost:{REDIS_PORT}/0",
    broker=f"redis://localhost:{REDIS_PORT}/0"
)


@celery.task(
    queue="analysis",
    autoretry_for=(ConnectionResetError, FileNotFoundError, BlockingIOError)
)
def background_analysis_384(plate_list):
    """check for new experiment directory"""
    time.sleep(10)
    plaque_assay.main.run(plate_list, plate=384)


@celery.task(
    queue="image_stitch",
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
