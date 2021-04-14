import logging
import os
import math
import string
from collections import defaultdict

from watchdog.events import LoggingEventHandler

import task
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
        variant_map = self.create_variant_mapping()
        self.variant_mapping = variant_map
        self.variant_mapping_rev = self.reverse_variant_mapping(variant_map)

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
        variant_letter = self.get_variant_letter(plate_name)
        self.handle_analysis(src_path, experiment, variant_letter)
        self.handle_stitching(src_path, experiment, plate_name)

    def handle_analysis(self, src_path, experiment, variant_letter):
        """
        Determine if valid and new exported data, and if so launches
        a new celery analysis task.

        Parameters:
        ------------
        src_path: string
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
        plate_list_96 = self.create_plate_list_96(experiment)
        plate_list_384 = self.create_plate_list_384(experiment, variant_letter)
        if len(plate_list_96) == 8:
            task.background_analysis_96.delay(plate_list_96)
            logging.info("analysis launched, adding to processed database")
            # TODO: move to end of celery task
            self.database.add_processed_experiment(experiment, variant_letter)
        if len(plate_list_384) == 2:
            task.background_analysis_384.delay(plate_list_384)
            logging.info("analysis launched, adding to processed database")
            # TODO: move to end of celery task
            self.database.add_processed_experiment(experiment, variant_letter)

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
            # TODO: move to end of celery task
            self.database.add_stitched_plate(plate_name)
            logging.info("stitching launched, adding to stitched database")
        else:
            logging.info("not a 384 plate, skipping stitching")

    def is_384_plate(self, dir_name, experiment):
        """determine if it's a 384-well plate"""
        final_path = os.path.basename(dir_name)
        parsed_experiment = final_path.split("__")[0][-6:]
        return final_path.startswith("S") and parsed_experiment == experiment

    def create_plate_list_96(self, experiment):
        """
        create a plate list from an experiment and variant names
        """
        all_subdirs = [i for i in os.listdir(self.input_dir)]
        full_paths = [os.path.join(self.input_dir, i) for i in all_subdirs]
        # filter to just those of the specific experiment
        wanted_experiment = []
        for i in full_paths:
            final_path = os.path.basename(i)
            # 96-well plates have the prefix "A11000000"
            if (
                final_path[3:9] == experiment
                and final_path[0] == "A"
                and final_path[1] in (1, 2, 3, 4)
            ):
                wanted_experiment.append(i)
        return wanted_experiment

    def create_plate_list_384(self, experiment, variant_letter):
        """
        create a plate list from an experiment and variant names
        """
        all_subdirs = [i for i in os.listdir(self.input_dir)]
        full_paths = [os.path.join(self.input_dir, i) for i in all_subdirs]
        # filter to just those of the specific experiment and variants
        variant_ints = self.get_variant_ints_from_letter(variant_letter)
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

    def get_experiment_name(self, dir_name):
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

    def get_plate_name(self, dir_name):
        """get the name of the plate from the full directory path"""
        plate_dir = os.path.basename(dir_name)
        return plate_dir.split("__")[0]

    def create_variant_mapping(self):
        """
        Create variant mapping dictionary, to map the paired sequential
        numbers to a variant letter.

        e.g:
            1, 2 => "a"
            3, 4 => "b"

        """
        # NOTE that at the moment this only goes up to z, so 26 different
        # variants, although it can possibly reach 49. We will need to figure
        # out how to handle 27+ if we ever reach that far.
        variant_dict = dict()
        for i in range(1, 27):
            letter_int = math.ceil(i / 2) - 1
            variant_dict[i] = string.ascii_lowercase[letter_int]
        return variant_dict

    def reverse_variant_mapping(self, variant_map):
        """
        reverse the variant map so we have the possible integers for a given
        variant letter
        e.g:
            {1: "a", 2: "a", 3: "b", 4: "b} => {"a": [1, 2], "b": [3, 4]}
        """
        variant_map_rev = defaultdict(list)
        for integer, letter in variant_map.items():
            variant_map_rev[letter].append(integer)
        return variant_map_rev

    def get_variant_letter(self, plate_name):
        """get variant letter from plate name"""
        variant_int = int(plate_name[1:3])
        if variant_int > 26:
            raise NotImplementedError(
                "MyEventHandler's variant mapping only handles variant numbers "
                + "up to 26. You will need to alter this to use high numbers"
            )
        return self.variant_mapping[variant_int]

    def get_variant_ints_from_letter(self, letter):
        """
        e.g:
        "a" => [1, 2]
        ...
        """
        return self.variant_mapping_rev[letter]
