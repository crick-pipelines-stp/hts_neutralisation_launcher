"""
module docstring
"""
import os
import logging
import sys
import textwrap
from typing import List

import db
from snapshot import Snapshot
import utils
import slack
import task


RESULTS_DIR = "/mnt/proj-c19/ABNEUTRALISATION/NA_raw_data"


class Dispatcher:

    def __init__(self, results_dir=RESULTS_DIR):
        self.results_dir = results_dir
        engine = db.create_engine()
        session = db.create_session(engine)
        self.database = db.Database(session)

    def get_new_directories(self) -> List[str]:
        snapshot = Snapshot(self.results_dir)
        if snapshot.current_hash == snapshot.stored_hash:
            logging.info("hash of NA_raw_data remains unchanged, exiting...")
            sys.exit(0)
        new_data = snapshot.get_new_dirs()
        if len(new_data) == 0:
            logging.info(
                "NA_raw_data has changed, but no new valid directories found, exiting..."
            )
            sys.exit(0)
        return new_data

    def create_plate_list(self, workflow_id, variant, titration=False):
        prefix_char = "T" if titration else "S"
        all_subdirs = [i for i in os.listdir(self.input_dir)]
        full_paths = [os.path.join(self.results_dir, i) for i in all_subdirs]
        variant_ints = self.database.get_variant_ints_from_name(variant)
        wanted_workflows = []
        for i in full_paths:
            final_path = os.path.basename(i)
            if (
                final_path[3:9] == workflow_id
                and final_path[0] == prefix_char
                and int(final_path[1:3]) in variant_ints
            ):
                wanted_workflows.append(i)
        return wanted_workflows

    def dispatch_plate(self, plate_path: str):
        plate_name = utils.get_plate_name(plate_path)
        is_titration = utils.is_titration_plate(plate_name)
        variant = self.database.get_variant_from_plate_name(
            plate_name, titration=is_titration
        )
        workflow_id = utils.get_workflow_id(plate_name)
        plate_list = self.create_plate_list(
            workflow_id, variant, titration=is_titration
        )
        if len(plate_list) != 2:
            logging.info("waiting for 2nd replicate plate, exiting...")
            return None
        self.handle_analysis(plate_list, workflow_id, variant, titration=is_titration)
        self.handle_stitching(plate_path, workflow_id, plate_name)

    def handle_analysis(
        self, plate_list: List[str], workflow_id: str, variant: str, titration=False
    ):
        """
        Determine if valid and new exported data, and if so launches
        a new celery analysis task.
        Parameters:
        ------------
        plate_list: list of plate paths from self.create_plate_list()
        workflow_id: string
        variant: string
        Returns:
        --------
        None
        """
        if titration:
            analysis_state = self.database.get_analysis_state(
                workflow_id, variant, titration=True
            )
        else:
            analysis_state = self.database.get_analysis_state(workflow_id, variant)
        if analysis_state == "finished":
            logging.info(
                f"workflow_id: {workflow_id} variant: {variant} has already been analysed"
            )
            return None
        elif analysis_state == "recent":
            logging.info(
                f"workflow_id: {workflow_id} variant: {variant} has recently been added to the job queue, skipping..."
            )
            return None
        elif analysis_state == "stuck":
            # reset create_at timestamp and resubmit to job queue
            logging.info(
                f"workflow_id: {workflow_id} variant: {variant} has old processed entry but not finished, resubmitting to job queue..."
            )
            if titration:
                self.database.update_titration_entry(workflow_id, variant)
            else:
                self.database.update_analysis_entry(workflow_id, variant)
            assert len(plate_list) == 2
            logging.info(f"both plates for {workflow_id}: {variant} found")
            if titration:
                task.background_titration_analysis_384(plate_list)
                logging.info("titration analysis launched")
            else:
                task.background_analysis_384.delay(plate_list)
                logging.info("analysis launched")
        elif analysis_state == "does not exist":
            logging.info(f"new workflow_id: {workflow_id} variant: {variant}")
            assert len(plate_list) == 2
            logging.info(f"both plates for {workflow_id}: {variant} found")
            if titration:
                self.database.create_titration_entry(workflow_id, variant)
                task.background_titration_analysis_384.delay(plate_list)
                logging.info("titration analysis launched")
            else:
                self.database.create_analysis_entry(workflow_id, variant)
                task.background_analysis_384.delay(plate_list)
                logging.info("analysis launched")
        else:
            logging.error(
                f"invalid analysis state {analysis_state}, sending slack alert"
            )
            message = textwrap.dedent(
                f"""
                Invalid analysis state ({analysis_state}) when checking with
                `Database.get_analysis_state()`.
                """
            )
            slack.send_simple_alert(workflow_id, variant, message)

    def handle_stitching(self, plate_path: str, workflow_id: str, plate_name: str):
        if not self.is_384_plate(plate_path, workflow_id):
            logging.warning("not a 384 plate, skipping stitching")
            return None
        logging.info(f"determined {plate_name} is a 384 plate")
        stitching_state = self.database.get_stitching_state(plate_name)
        if stitching_state == "finished":
            # already stitched, ignore
            logging.info(f"plate: {plate_name} has already been stitched")
        elif stitching_state == "recent":
            # recent, ignore
            logging.info(
                f"plate: {plate_name} has recently been submitted, skipping..."
            )
        elif stitching_state == "stuck":
            # reset create_at timestamp and resubmit to job queue
            logging.info(
                f"plate: {plate_name} has old processed entry but not finished, resubmitting to job queue..."
            )
            self.database.update_stitching_entry(plate_name)
            indexfile_path = os.path.join(plate_path, "indexfile.txt")
            task.background_image_stitch_384.delay(indexfile_path)
            logging.info(f"stitching launched for plate: {plate_name}")
        elif stitching_state == "does not exist":
            # create new entry and submit to job queue
            self.database.create_stitching_entry(plate_name)
            indexfile_path = os.path.join(plate_path, "indexfile.txt")
            task.background_image_stitch_384.delay(indexfile_path)
            logging.info(f"stitching launched for plate: {plate_name}")
        else:
            logging.error(
                f"invalid stitching state {stitching_state}, sending slack alert"
            )
            message = textwrap.dedent(
                f"""
                Invalid stitching state ({stitching_state}) when checking with
                `Database.get_stitching_state()`.
                """
            )
            slack.send_simple_warning(message)


def main():
    dispatch = Dispatcher()
    new_plates = dispatch.get_new_directories()
    for plate in new_plates:
        dispatch.dispatch_plate(plate)


if __name__ == "__main__":
    main()
