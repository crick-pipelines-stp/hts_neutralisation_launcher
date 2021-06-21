import logging
import os
import math
import string
from collections import defaultdict

from watchdog.events import LoggingEventHandler

import task
from variant_mapper import VariantMapper
from db import Database


class MyEventHandler(LoggingEventHandler):
    """
    A watchdog event handler, to launch celery
    tasks when new exported data is detected
    """

    def __init__(self, input_dir, db_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_dir = input_dir
        self.db_path = db_path
        self.database = Database(db_path)
        self.database.create()
        self.variant_mapper = VariantMapper()

    def on_created(self, event):
        """
        Override default LoggingEventHandler.on_created method.

        When a directory is created, this method will determine if the
        directory path looks like it's been exported from the phenix, and
        if so will pass the directory path to the analysis and image-stitching
        celery tasks.
        """
        super().on_created(event)
        src_path = event.src_path
        experiment = self.get_experiment_name(src_path)
        if experiment is None:
            # invalid experiment name, skip
            return None
        plate_name = self.get_plate_name(src_path)
        variant_letter = self.variant_mapper.get_variant_letter(plate_name)
        self.handle_analysis(experiment, variant_letter)
        self.handle_stitching(src_path, experiment, plate_name)

    def handle_analysis(self, experiment, variant_letter):
        """
        Determine if valid and new exported data, and if so launches
        a new celery analysis task.

        Parameters:
        ------------
        experiment: string
        variant_letter: string

        Returns:
        --------
        None
        """
        if self.database.is_experiment_processed(experiment, variant_letter):
            logging.info(
                f"experiment: {experiment} variant: {variant_letter} has already been analysed"
            )
            return None
        logging.info(f"new experiment: {experiment} variant: {variant_letter}")
        plate_list_384 = self.create_plate_list_384(experiment, variant_letter)
        if len(plate_list_384) == 2:
            task.background_analysis_384.delay(plate_list_384)
            logging.info("analysis launched")

    def handle_stitching(self, src_path, experiment, plate_name):
        """
        Determine if valid and new exported data, and if so launches
        a new celery image-stitching task.

        Parameters:
        ------------
        src_path: string
        experiment: string
        plate_name: string

        Returns:
        --------
        None
        """
        if self.is_384_plate(src_path, experiment):
            logging.info("determined it's a 384 plate")
            if self.database.is_plate_stitched(plate_name):
                logging.info(f"plate {plate_name} has already been stitched")
                return None
            logging.info(f"new plate {plate_name}")
            indexfile_path = os.path.join(src_path, "indexfile.txt")
            task.background_image_stitch_384.delay(indexfile_path)
            logging.info("stitching launched")
        else:
            logging.info("not a 384 plate, skipping stitching")

    @staticmethod
    def is_384_plate(dir_name, experiment):
        """determine if it's a 384-well plate"""
        final_path = os.path.basename(dir_name)
        parsed_experiment = final_path.split("__")[0][-6:]
        return final_path.startswith("S") and parsed_experiment == experiment

    def create_plate_list_384(self, experiment, variant_letter):
        """
        create a plate list from an experiment and variant names
        """
        all_subdirs = [i for i in os.listdir(self.input_dir)]
        full_paths = [os.path.join(self.input_dir, i) for i in all_subdirs]
        # filter to just those of the specific experiment and variants
        variant_ints = self.variant_mapper.get_variant_ints_from_letter(variant_letter)
        wanted_experiment = []
        for i in full_paths:
            final_path = os.path.basename(i)
            # 384-well plates have the prefix "S01000000"
            if (
                final_path[3:9] == experiment
                and final_path[0] == "S"
                and int(final_path[1:3]) in variant_ints
            ):
                wanted_experiment.append(i)
        return wanted_experiment

    @staticmethod
    def get_experiment_name(dir_name):
        """get the name of the experiment from a plate directory"""
        plate_dir = os.path.basename(dir_name)
        if plate_dir.startswith("A"):
            experiment_name = plate_dir.split("__")[0][-6:]
        elif plate_dir.startswith("S"):
            # 384-well plates should be the same for now
            experiment_name = plate_dir.split("__")[0][-6:]
        else:
            logging.error(f"invalid plate directory name {plate_dir}, skipping")
            experiment_name = None
        return experiment_name

    @staticmethod
    def get_plate_name(dir_name):
        """get the name of the plate from the full directory path"""
        plate_dir = os.path.basename(dir_name)
        return plate_dir.split("__")[0]
