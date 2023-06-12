import os
import time
from urllib.error import URLError, HTTPError

import sqlalchemy.exc
import celery

import db
import plaque_assay
import stitch_images
import slack
from config import parse_config


cfg_celery = parse_config()["celery"]


celery = celery.Celery(
    "task",
    backend=cfg_celery["backend"],
    broker=cfg_celery["broker"],
)


class BaseTask(celery.Task):
    def on_success(self, retval, task_id, args, kwargs):
        """
        update database to record already-run
        analysis and image-stitching.
        """
        engine = db.create_engine()
        session = db.create_session(engine)
        database = db.Database(session)
        task_type = self.get_task_type(args)
        if task_type == "analysis":
            workflow_id = self.get_workflow(args)
            variant = self.get_variant(args, database)
            database.mark_analysis_entry_as_finished(workflow_id, variant)
        if task_type == "stitching":
            plate_name = self.get_plate_name_stitch(args)
            database.mark_stitching_entry_as_finished(plate_name)
        if task_type == "titration":
            workflow_id = self.get_workflow(args)
            variant = self.get_variant(args, database, titration=True)
            database.mark_titration_entry_as_finished(workflow_id, variant)

    def get_task_type(self, args):
        """
        determine whether task was analysis, stitching, or titration
        based on the arguments given to it.

         args is always a tuple
         for stitching tasks it's a tuple of 1 string
             e.g   ("/camp/ABNEUTRALISATION..../indexfile.txt", )
         for analysis tasks it's a tuple of 1 list containing 2 strings
            e.g (["/camp/.../", "/camp/.../"], )
        """
        assert len(args) == 1
        if isinstance(args[0], str):
            task_type = "stitching"
        elif isinstance(args[0], list) and len(args[0]) == 2:
            # determine if analysis or titration task
            plate_names = [self.get_plate_name(i) for i in args[0]]
            titration_plate_count = sum(self.is_titration_plate(i) for i in plate_names)
            if titration_plate_count == 2:
                task_type = "titration"
            elif titration_plate_count == 0:
                task_type = "analysis"
            else:
                raise RuntimeError(f"invalid args: {args}")
        else:
            raise RuntimeError(f"invalid args: {args}")
        return task_type

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """send slack alert on task failure"""
        slack.send_alert(exc, task_id, args, kwargs, einfo)

    @staticmethod
    def is_titration_plate(plate_name):
        """determines if plate name is from a titration plate"""
        return os.path.basename(plate_name).startswith("T")

    @staticmethod
    def get_plate_name(path):
        """get plate name from path"""
        return os.path.basename(path).split("__")[0]

    @staticmethod
    def get_plate_name_stitch(args):
        """
        get plate name from image_stitch args

        Arguments:
        -----------
        args: tuple of length 1
            e.g ('/.../S06000114__2021-05-14T13_44_30-Measurement 1/indexfile.txt', )

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
    def get_variant(args, database, titration=False):
        """get variant letter short-code from args"""
        paths = args[0]
        assert len(paths) == 2
        variant_set = set()
        for path in paths:
            plate_name = os.path.basename(path).split("__")[0]
            variant = database.get_variant_from_plate_name(plate_name, titration)
            variant_set.add(variant)
        assert len(variant_set) == 1, "multiple variants detected"
        variant_name = list(variant_set)[0]
        return variant_name


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
        sqlalchemy.exc.OperationalError,
    ),
)
def background_image_stitch_384(indexfile_path):
    """image stitching for 384 well plate"""
    time.sleep(10)
    stitcher = stitch_images.ImageStitcher(indexfile_path)
    stitcher.stitch_plate()
    stitcher.stitch_all_samples()
    stitcher.save_all()
    missing = stitcher.collect_missing_images()
    if missing:
        slack.send_warning(f"Missing images: {indexfile_path} {missing}")


@celery.task(
    queue="image_stitch_titration",
    base=BaseTask,
    autoretry_for=(
        ConnectionResetError,
        FileNotFoundError,
        URLError,
        HTTPError,
        BlockingIOError,
        sqlalchemy.exc.OperationalError,
    ),
)
def background_image_stitch_titration_384(indexfile_path):
    """image stitching for 384 well plate"""
    time.sleep(10)
    stitcher = stitch_images.ImageStitcher(indexfile_path)
    stitcher.stitch_plate()
    stitcher.save_plates()
    missing = stitcher.collect_missing_images()
    if missing:
        slack.send_warning(f"Missing images: {indexfile_path} {missing}")


@celery.task(
    queue="titration",
    base=BaseTask,
    autoretry_for=(
        ConnectionResetError,
        FileNotFoundError,
        BlockingIOError,
        sqlalchemy.exc.OperationalError,
    ),
)
def background_titration_analysis_384(plate_list):
    """titration analysis"""
    time.sleep(10)
    plaque_assay.titration_main.run(plate_list)
