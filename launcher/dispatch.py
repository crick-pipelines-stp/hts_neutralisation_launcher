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

log = logging.getLogger(__name__)


RESULTS_DIR = "/mnt/proj-c19/ABNEUTRALISATION/NA_raw_data"
SNAPSHOT_DB = "/home/warchas/launcher/.snapshot.db"


class Dispatcher:
    def __init__(self, results_dir=RESULTS_DIR, db_path=SNAPSHOT_DB, titration=False):
        self.results_dir = results_dir
        self.db_path = db_path
        engine = db.create_engine()
        session = db.create_session(engine)
        self.prefix_char = "T" if titration else "S"
        self.regex_filter = rf"^{self.prefix_char}.*/*Measurement [0-9]$"
        self.database = db.Database(session)

    def get_new_directories(self) -> List[str]:
        snapshot = Snapshot(self.results_dir, self.db_path, regex=self.regex_filter)
        if snapshot.current_hash == snapshot.stored_hash:
            log.info(
                f"hash of {self.results_dir} contents remains unchanged, exiting..."
            )
            sys.exit(0)
        new_data = snapshot.get_new_dirs()
        if len(new_data) == 0:
            log.info(
                f"{self.results_dir} has changed, but no new valid directories found, exiting..."
            )
            snapshot.make_snapshot()
            sys.exit(0)
        snapshot.make_snapshot()
        return new_data

    def create_plate_list(self, workflow_id, variant):
        all_subdirs = [i for i in os.listdir(self.results_dir)]
        full_paths = [os.path.join(self.results_dir, i) for i in all_subdirs]
        variant_ints = self.database.get_variant_ints_from_name(variant)
        wanted_workflows = []
        for i in full_paths:
            final_path = os.path.basename(i)
            plate_name = utils.get_plate_name(final_path)
            if (
                plate_name[-6:] == workflow_id
                and final_path[0] == self.prefix_char
                and int(final_path[1:3]) in variant_ints
            ):
                wanted_workflows.append(i)
        return wanted_workflows

    def dispatch_plate(self, plate_path: str):
        plate_name = utils.get_plate_name(plate_path)
        workflow_id = utils.get_workflow_id(plate_name)
        is_titration = utils.is_titration_plate(plate_name)
        variant = self.database.get_variant_from_plate_name(
            plate_name, titration=is_titration
        )
        self.handle_stitching(plate_path, workflow_id, plate_name, is_titration)
        plate_list = self.create_plate_list(workflow_id, variant)
        log.info(f"plate_list = {plate_list}")
        if len(plate_list) == 2:
            self.handle_analysis(
                plate_list, workflow_id, variant, titration=is_titration
            )

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
        analysis_state = self.database.get_analysis_state(
            workflow_id, variant, titration=titration
        )
        if analysis_state == "finished":
            log.info(
                f"workflow_id: {workflow_id} variant: {variant} has already been analysed"
            )
        elif analysis_state == "recent":
            log.info(
                f"workflow_id: {workflow_id} variant: {variant} has recently been added to the job queue, skipping..."
            )
        elif analysis_state == "stuck":
            # reset create_at timestamp and resubmit to job queue
            log.info(
                f"workflow_id: {workflow_id} variant: {variant} has old processed entry but not finished, resubmitting to job queue..."
            )
            if titration:
                self.database.update_titration_entry(workflow_id, variant)
            else:
                self.database.update_analysis_entry(workflow_id, variant)
            assert len(plate_list) == 2
            log.info(f"both plates for {workflow_id}: {variant} found")
            if titration:
                task.background_titration_analysis_384(plate_list)
                log.info("titration analysis launched")
            else:
                task.background_analysis_384.delay(plate_list)
                log.info("analysis launched")
        elif analysis_state == "does not exist":
            log.info(f"new workflow_id: {workflow_id} variant: {variant}")
            assert len(plate_list) == 2
            log.info(f"both plates for {workflow_id}: {variant} found")
            if titration:
                self.database.create_titration_entry(workflow_id, variant)
                task.background_titration_analysis_384.delay(plate_list)
                log.info("titration analysis launched")
            else:
                self.database.create_analysis_entry(workflow_id, variant)
                task.background_analysis_384.delay(plate_list)
                log.info("analysis launched")
        else:
            log.error(f"invalid analysis state {analysis_state}, sending slack alert")
            message = textwrap.dedent(
                f"""
                Invalid analysis state ({analysis_state}) when checking with
                `Database.get_analysis_state()`.
                """
            )
            slack.send_simple_alert(workflow_id, variant, message)

    def handle_stitching(
        self, plate_path: str, workflow_id: str, plate_name: str, is_titration: bool
    ):
        if not utils.is_384_well_plate(plate_path, workflow_id):
            log.warning("not a 384 plate, skipping stitching")
            return None
        log.info(f"determined {plate_name} is a 384 plate")
        stitching_state = self.database.get_stitching_state(plate_name)
        if stitching_state == "finished":
            # already stitched, ignore
            log.info(f"plate: {plate_name} has already been stitched")
        elif stitching_state == "recent":
            # recent, ignore
            log.info(f"plate: {plate_name} has recently been submitted, skipping...")
        elif stitching_state == "stuck":
            # reset create_at timestamp and resubmit to job queue
            log.info(
                f"plate: {plate_name} has old processed entry but not finished, resubmitting to job queue..."
            )
            self.database.update_stitching_entry(plate_name)
            indexfile_path = os.path.join(plate_path, "indexfile.txt")
            if is_titration:
                task.background_image_stitch_titration_384.delay(indexfile_path)
            else:
                task.background_image_stitch_384.delay(indexfile_path)
            log.info(f"stitching launched for plate: {plate_name}")
        elif stitching_state == "does not exist":
            # create new entry and submit to job queue
            self.database.create_stitching_entry(plate_name)
            indexfile_path = os.path.join(plate_path, "indexfile.txt")
            if is_titration:
                task.background_image_stitch_titration_384.delay(indexfile_path)
            else:
                task.background_image_stitch_384.delay(indexfile_path)
            log.info(f"stitching launched for plate: {plate_name}")
        else:
            log.error(f"invalid stitching state {stitching_state}, sending slack alert")
            message = textwrap.dedent(
                f"""
                Invalid stitching state ({stitching_state}) when checking with
                `Database.get_stitching_state()`.
                """
            )
            slack.send_warning(message)
