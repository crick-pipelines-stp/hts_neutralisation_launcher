import os
import logging
import sqlite3
import sys
import time

from watchdog.events import LoggingEventHandler
from watchdog.observers.polling import PollingObserver as Observer

import task


class MyEventHandler(LoggingEventHandler):
    def __init__(self, input_dir, db_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_dir = input_dir
        self.db_path = db_path
        self.create_db()

    def on_created(self, event):
        """
        over-ride method when file or directory is created
        """
        super().on_created(event)
        src_path = event.src_path
        experiment = self.get_experiment_name(src_path)
        if experiment is None:
            # invalid experiment name, skip
            return None
        #### concentration-response analysis ###
        if self.experiment_exists(experiment):
            logging.info(
                f"experiment {experiment} already exists in processed database"
            )
        else:
            logging.info(f"new experiment: {experiment}")
            logging.info("creating plate_list")
            plate_list_96 = self.create_plate_list_96(experiment)
            plate_list_384 = self.create_plate_list_384(experiment)
            if len(plate_list_96) == 8:
                logging.info("launching analysis job")
                task.background_analysis_96.delay(plate_list_96)
                logging.info("analysis complete, adding to processed database")
                self.add_experiment_to_db(experiment)
            else:
                logging.warning(
                    f"plate list 96 length = {len(plate_list_96)} expected 8"
                )
            if len(plate_list_384) == 2:
                logging.info("launching analysis job")
                task.background_analysis_384.delay(plate_list_384)
                logging.info("analysis complete, adding to processed database")
                self.add_experiment_to_db(experiment)
            else:
                logging.warning(
                    f"plate list 384 length = {len(plate_list_384)} expected 2"
                )
        #### image stitching ####
        if self.is_384_plate(src_path, experiment):
            logging.info("determined 384 plate, stitching images")
            indexfile_path = os.path.join(src_path, "indexfile.txt")
            task.background_image_stitch_384.delay(indexfile_path)
        else:
            logging.info("not a 384 plate")

    def is_384_plate(self, dir_name, experiment):
        """determine if it's a 384-well plate"""
        final_path = os.path.basename(dir_name)
        parsed_experiment = final_path.split("__")[0][-6:]
        if final_path.startswith("S") and parsed_experiment == experiment:
            return True
        else:
            return False

    def create_plate_list_96(self, experiment):
        """
        create a plate list from an experiment name
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

    def create_plate_list_384(self, experiment):
        """
        create a plate list from an experiment name
        """
        all_subdirs = [i for i in os.listdir(self.input_dir)]
        full_paths = [os.path.join(self.input_dir, i) for i in all_subdirs]
        # filter to just those of the specific experiment
        wanted_experiment = []
        for i in full_paths:
            final_path = os.path.basename(i)
            # 384-well plates have the prefix "S01000000"
            if final_path[3:9] == experiment and final_path[0] == "S":
                wanted_experiment.append(i)
        return wanted_experiment

    def get_experiment_name(self, dir_name):
        """
        get the name of the experiment from a plate directory
        """
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

    def experiment_exists(self, experiment):
        """
        check if an experiment is already in the processed database
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM processed WHERE experiment=(?))",
            (experiment,),
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def add_experiment_to_db(self, experiment):
        """add an experiment to the processed database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO processed (experiment) VALUES (?);", (experiment,))
        conn.commit()
        cursor.close()

    def create_db(self):
        """
        create processed experiments database if it doesn't
        already exist
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS processed
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment CHAR(10) NOT NULL
            );
            """
        )
        conn.commit()
        cursor.close()


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(
                "/camp/hts/working/scott/neutralisation_watchdog.log",
                mode="a"
            ),
            logging.StreamHandler(sys.stdout)
        ]
    )

    input_dir = "/camp/hts/working/Neutralisation Assay/384_raw_data"

    db_path = "processed_experiments.sqlite"

    event_handler = MyEventHandler(input_dir, db_path)

    observer = Observer()
    observer.schedule(event_handler, input_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
