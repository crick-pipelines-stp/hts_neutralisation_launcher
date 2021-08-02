import os
import time
from urllib.error import URLError, HTTPError

import sqlalchemy.exc
import celery

import db
import plaque_assay
import stitch_images
import utils
from variant_mapper import VariantMapper


REDIS_PORT = 6379
DB_PATH = os.path.expanduser("~/hts_neutralisation_launcher/processed_experiments.sqlite")


celery = celery.Celery(
    "task",
    backend=f"redis://localhost:{REDIS_PORT}/0",
    broker=f"redis://localhost:{REDIS_PORT}/0",
)


class BaseTask(celery.Task):

    def on_success(self, retval, task_id, args, kwargs):
        """
        update sqlite database to record already-run
        analysis and image-stitching.
        """
        database = db.Database(path=DB_PATH)
        # args is always a tuple
        # for stitching tasks it's a tuple of 1 string
        #     e.g   ("/camp/ABNEUTRALISATION..../indexfile.txt", )
        # for analysis tasks it's a tuple of 1 list containing 2 strings
        #    e.g (["/camp/.../", "/camp/.../"], )
        assert len(args) == 1
        if isinstance(args[0], str):
            task_type = "stitching"
        elif isinstance(args[0], list) and len(args[0]) == 2:
            task_type = "analysis"
        else:
            raise RuntimeError(f"invalid args: {args}")
        if task_type == "analysis":
            workflow_id = self.get_workflow(args)
            variant = self.get_variant(args)
            database.mark_analysis_entry_as_finished(workflow_id, variant)
        if task_type == "stitching":
            plate_name = self.get_plate_name(args)
            database.add_stitched_plate(plate_name)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """send slack alert on task failure"""
        print("sending slack alert")
        status_code = utils.send_slack_alert(exc, task_id, args, kwargs, einfo)
        if status_code != 200:
            print(f"{status_code}: failed to send slack alert")

    @staticmethod
    def get_plate_name(args):
        """
        get plate name from image_stitch args

        Arguments:
        -----------
        args: tuple of 1 string
            e.g ('/.../S06000114__2021-05-14T13_44_30-Measurement 1/indexfile.txt',)

        Returns:
        ---------
        string e.g "S06000114"
        """
        path = args[0]
        plate_dir = path.split(os.sep)[-2]
        plate_name = plate_dir.split("__")[0]
        return plate_name

    @staticmethod
    def get_workflow(args):
        """get workflow from args"""
        paths = args[0]
        assert len(paths) == 2
        # both workflows in args are the same
        workflow_set = set()
        for path in paths:
            workflow = os.path.basename(path).split("__")[0][-6:]
            workflow_set.add(workflow)
        assert len(workflow_set) == 1, "multiple workflows detected"
        workflow_single = list(workflow_set)[0]
        return workflow_single

    @staticmethod
    def get_variant(args):
        """get variant letter short-code from args"""
        paths = args[0]
        assert len(paths) == 2
        variant_mapper = VariantMapper()
        variant_letters = set()
        for path in paths:
            plate_name = os.path.basename(path).split("__")[0]
            letter = variant_mapper.get_variant_letter(plate_name)
            variant_letters.add(letter)
        assert len(variant_letters) == 1, "multiple variants detected"
        variant_letter = list(variant_letters)[0]
        return variant_letter


@celery.task(
    queue="analysis",
    base=BaseTask,
    autoretry_for=(
        ConnectionResetError,
        FileNotFoundError,
        BlockingIOError,
        sqlalchemy.exc.OperationalError,
    ),
)
def background_analysis_384(plate_list):
    """check for new experiment directory"""
    time.sleep(10)
    plaque_assay.main.run(plate_list)


@celery.task(
    queue="image_stitch",
    base=BaseTask,
    autoretry_for=(
        ConnectionResetError,
        FileNotFoundError,
        URLError,
        HTTPError,
        BlockingIOError,
    ),
)
def background_image_stitch_384(indexfile_path):
    """image stitching for 384 well plate"""
    time.sleep(10)
    stitcher = stitch_images.ImageStitcher(indexfile_path)
    stitcher.stitch_plate()
    stitcher.stitch_all_samples()
    stitcher.save_all()
