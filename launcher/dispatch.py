"""
module docstring
"""

import logging
import os
import sys
import textwrap
from typing import List

import db
import slack
import task
import utils
from config import parse_config
from db import AnalysisState, VariantLookupError
from snapshot import Snapshot

log = logging.getLogger(__name__)
cfg_analysis = parse_config()["analysis"]


RESULTS_DIR = cfg_analysis["results_dir"]
SNAPSHOT_DB = cfg_analysis["snapshot_db"]


class Dispatcher:
    """
    Most of the logic for detecting whether to submit analysis and image
    stitching jobs to celery task queue.
    """

    def __init__(
        self,
        results_dir: str = RESULTS_DIR,
        db_path: str = SNAPSHOT_DB,
    ):
        self.results_dir = results_dir
        self.db_path = db_path
        engine = db.create_engine()
        session = db.create_session(engine)
        self.regex_filter = r"^[A-Z][0-9]{8}_.*-Measurement [0-9]$"
        self.database = db.Database(session)

    def get_new_directories(self) -> List[str]:
        """
        Uses snapshotting to get new directories (if present).
        This returns a list of all the new directories which match the given
        regex filter, which may contain multiple workflows and variants, and
        may not contain a matching replicate plate.
        If no new valid directories are found, the entire process exits with
        an exit code 0.
        """
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

    def create_plate_list(self, workflow_id: str, variant: str) -> List[str]:
        """
        Given a workflow and variant, this will find any plates in the results
        directory that match.
        It is inefficient, compared to simply pairing up replicate plates from
        `self.get_new_directories()`, but it is done this way to account
        for when replicate pairs are not exported at the same time.
        """
        all_subdirs = os.listdir(self.results_dir)
        full_paths = sorted([os.path.join(self.results_dir, i) for i in all_subdirs])
        variant_ints = self.database.get_variant_ints_from_name(variant)
        wanted_workflows = []
        for path in full_paths:
            if self.is_matching_plate(path, workflow_id, variant_ints):
                wanted_workflows.append(path)
                if len(wanted_workflows) == 2:
                    # already found both plates, no point continuing, exit early
                    break
        return wanted_workflows

    def is_matching_plate(
        self, path: str, workflow_id: str, variants: List[int]
    ) -> bool:
        """
        Determine if a plate path matches a given workflow_id + variant.
        Variant info is passed as a list integers, e.g [1, 2] is "England2".
        """
        final_path = os.path.basename(path)
        plate_name = utils.get_plate_name(final_path)
        return plate_name[-6:] == workflow_id and int(final_path[1:3]) in variants

    def dispatch_plate(self, plate_path: str) -> None:
        """
        Given a single plate path, create image stitching job.
        Then look if there is a matching replicate plate, if so create
        analysis job.
        """
        plate_name = utils.get_plate_name(plate_path)
        workflow_id = utils.get_workflow_id(plate_name)
        is_titration = utils.is_titration_plate(plate_name)
        try:
            variant = self.database.get_variant_from_plate_name(
                plate_name, is_titration=is_titration
            )
        except VariantLookupError as err:
            log.error(err)
            return
        self.handle_stitching(plate_path, workflow_id, plate_name, is_titration)
        plate_list = self.create_plate_list(workflow_id, variant)
        log.info(f"plate_list = {plate_list}")
        if len(plate_list) == 2:
            self.handle_analysis(
                plate_list, workflow_id, variant, is_titration=is_titration
            )

    def handle_analysis(
        self, plate_list: List[str], workflow_id: str, variant: str, is_titration=False
    ) -> None:
        """
        Determine if valid and new exported data, and if so launches
        a new celery analysis task.
        This checks if an analysis job has already been submitted for a pair
        of plates. This is a redundant check as plaque_assay will not upload
        duplicated results for a given workflow_id + variant.
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
            workflow_id, variant, is_titration=is_titration
        )
        if analysis_state == AnalysisState.FINISHED:
            log.info(
                f"workflow_id: {workflow_id} variant: {variant} has already been analysed"
            )
        elif analysis_state == AnalysisState.RECENT:
            log.info(
                f"workflow_id: {workflow_id} variant: {variant} has recently been added to the job queue, skipping..."
            )
        elif analysis_state == AnalysisState.STALE:
            # reset create_at timestamp and resubmit to job queue
            log.info(
                f"workflow_id: {workflow_id} variant: {variant} is stale, resubmitting to job queue..."
            )
            if is_titration:
                self.database.update_titration_entry(workflow_id, variant)
                task.background_titration_analysis_384(plate_list)
                log.info("titration analysis launched")
            else:
                self.database.update_analysis_entry(workflow_id, variant)
                task.background_analysis_384.delay(plate_list)
                log.info("analysis launched")
        elif analysis_state == AnalysisState.NEW:
            log.info(f"new workflow_id: {workflow_id} variant: {variant}")
            log.info(f"both plates for {workflow_id}: {variant} found")
            if is_titration:
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
    ) -> None:
        if not utils.is_384_well_plate(plate_path, workflow_id):
            log.warning("not a 384 plate, skipping stitching")
            return None
        stitching_state = self.database.get_stitching_state(plate_name)
        if stitching_state == AnalysisState.FINISHED:
            # already stitched, ignore
            log.info(f"plate: {plate_name} has already been stitched, skipping...")
        elif stitching_state == AnalysisState.RECENT:
            # recent, ignore
            log.info(f"plate: {plate_name} has recently been submitted, skipping...")
        elif stitching_state == "stale":
            # reset created_at timestamp and resubmit to job queue
            log.info(f"plate: {plate_name} is stale")
            self.database.update_stitching_entry(plate_name)
            indexfile_path = os.path.join(plate_path, "indexfile.txt")
            if is_titration:
                task.background_image_stitch_titration_384.delay(indexfile_path)
            else:
                task.background_image_stitch_384.delay(indexfile_path)
            log.info(
                f"stitching launched for plate: {plate_name} has been resubmitted to the job queue"
            )
        elif stitching_state == AnalysisState.NEW:
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
