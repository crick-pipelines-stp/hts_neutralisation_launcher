import logging
import os
import textwrap

from watchdog.events import LoggingEventHandler

import task
import utils
import db


class MyEventHandler(LoggingEventHandler):
    """
    A watchdog event handler, to launch celery
    tasks when new exported data is detected
    """

    def __init__(self, input_dir, db_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_dir = input_dir
        self.db_path = db_path
        engine = db.create_engine()
        session = db.create_session(engine)
        self.database = db.Database(session)

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
        workflow_id = self.get_workflow_id(src_path)
        if workflow_id is None:
            # invalid experiment name, skip
            return None
        plate_name = self.get_plate_name(src_path)
        try:
            variant = self.database.get_variant_from_plate_name(plate_name)
        except ValueError as e:
            logging.error(e)
            return None
        if self.is_titration_plate(plate_name):
            logging.info(f"plate: {plate_name} detected as a titration plate")
            # TODO: check we have both replicate plates
            plate_list_titration = self.create_plate_list_384(
                workflow_id, variant, titration=True
            )
            if len(plate_list_titration) == 2:
                self.handle_titration(plate_list_titration, workflow_id, variant)
            # return as to NOT submit analysis or stitching tasks
            return None
        plate_list_384 = self.create_plate_list_384(workflow_id, variant)
        # check both duplicates have been exported
        if len(plate_list_384) == 2:
            self.handle_analysis(plate_list_384, workflow_id, variant)
        self.handle_stitching(src_path, workflow_id, plate_name)

    def handle_analysis(self, plate_list_384, workflow_id, variant):
        """
        Determine if valid and new exported data, and if so launches
        a new celery analysis task.

        Parameters:
        ------------
        plate_list: list of plate paths from self.create_plate_list_384()
        workflow_id: string
        variant: string

        Returns:
        --------
        None
        """
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
            logging.info(f"workflow_id: {workflow_id} variant: {variant} has old processed entry but not finished, resubmitting to job queue...")
            self.database.update_analysis_entry(workflow_id, variant)
            assert len(plate_list_384) == 2
            logging.info(f"both plates for {workflow_id}: {variant} found")
            task.background_analysis_384.delay(plate_list_384)
            logging.info("analysis launched")
        elif analysis_state == "does not exist":
            logging.info(f"new workflow_id: {workflow_id} variant: {variant}")
            assert len(plate_list_384) == 2
            logging.info(f"both plates for {workflow_id}: {variant} found")
            self.database.create_analysis_entry(workflow_id, variant)
            task.background_analysis_384.delay(plate_list_384)
            logging.info("analysis launched")
        else:
            logging.error(f"invalid analysis state {analysis_state}, sending slack alert")
            message = textwrap.dedent(
                f"""
                Invalid analysis state ({analysis_state}) when checking with
                `Database.get_analysis_state()`.
                """
            )
            return_code = utils.send_simple_slack_alert(workflow_id, variant, message)
            if return_code != 200:
                logging.error(f"{return_code}: failed to send slack alert")
            return None

    def handle_stitching(self, src_path, workflow_id, plate_name):
        """
        Determine if valid and new exported data, and if so launches
        a new celery image-stitching task.

        Parameters:
        ------------
        src_path: string
        workflow_id: string
        plate_name: string

        Returns:
        --------
        None
        """
        if not self.is_384_plate(src_path, workflow_id):
            logging.warning("not a 384 plate, skipping stitching")
            return None
        logging.info(f"determined {plate_name} is a 384 plate")
        stitching_state = self.database.get_stitching_state(plate_name)
        if stitching_state == "finished":
            # already stitched, ignore
            logging.info(f"plate: {plate_name} has already been stitched")
        elif stitching_state == "recent":
            # recent, ignore
            logging.info(f"plate: {plate_name} has recently been submitted, skipping...")
        elif stitching_state == "stuck":
            # reset create_at timestamp and resubmit to job queue
            logging.info(f"plate: {plate_name} has old processed entry but not finished, resubmitting to job queue...")
            self.database.update_stitching_entry(plate_name)
            indexfile_path = os.path.join(src_path, "indexfile.txt")
            task.background_image_stitch_384.delay(indexfile_path)
            logging.info(f"stitching launched for plate: {plate_name}")
        elif stitching_state == "does not exist":
            # create new entry and submit to job queue
            self.database.create_stitching_entry(plate_name)
            indexfile_path = os.path.join(src_path, "indexfile.txt")
            task.background_image_stitch_384.delay(indexfile_path)
            logging.info(f"stitching launched for plate: {plate_name}")
        else:
            logging.error(f"invalid stitching state {stitching_state}, sending slack alert")
            message = textwrap.dedent(
                f"""
                Invalid stitching state ({stitching_state}) when checking with
                `Database.get_stitching_state()`.
                """
            )
            return_code = utils.send_simple_slack_warning(message)
            if return_code != 200:
                logging.error(f"{return_code}: failed to send slack alert")

    def handle_titration(self, plate_list_384, workflow_id, variant):
        """
        Determine if valid and new exported data, and if so launches
        a new celery titration analysis

        Parameters:
        ------------
        plate_list: list of plate paths from self.create_plate_list_384()
        workflow_id: string
        plate_name: string

        Returns:
        --------
        None
        """
        # TODO: make this
        raise NotImplementedError("not made this yet!")
        titration_state = self.database.get_titration_state(workflow_id, variant)
        if titration_state == "finished":
            logging.info(
                f"workflow_id: {workflow_id} variant: {variant} has already been analysed"
            )
            return None
        elif titration_state == "recent":
            logging.info(
                f"workflow_id: {workflow_id} variant: {variant} has recently been added to the job queue, skipping..."
            )
            return None
        elif titration_state == "stuck":
            # reset create_at timestamp and resubmit to job queue
            logging.info(f"workflow_id: {workflow_id} variant: {variant} has old processed entry but not finished, resubmitting to job queue...")
            self.database.update_titration_entry(workflow_id, variant)
            assert len(plate_list_384) == 2
            logging.info(f"both plates for {workflow_id}: {variant} found")
            task.background_titration_analysis_384.delay(plate_list_384)
            logging.info("titration analysis launched")
        elif titration_state == "does not exist":
            logging.info(f"new workflow_id: {workflow_id} variant: {variant}")
            assert len(plate_list_384) == 2
            logging.info(f"both plates for {workflow_id}: {variant} found")
            self.database.create_titration_entry(workflow_id, variant)
            task.background_titration_analysis_384.delay(plate_list_384)
            logging.info("titration analysis launched")
        else:
            logging.error(f"invalid titration analysis state {titration_state}, sending slack alert")
            message = textwrap.dedent(
                f"""
                Invalid titration analysis state ({titration_state}) when checking with
                `Database.get_titration_state()`.
                """
            )
            return_code = utils.send_simple_slack_alert(workflow_id, variant, message)
            if return_code != 200:
                logging.error(f"{return_code}: failed to send slack alert")
            return None

    @staticmethod
    def is_384_plate(dir_name, workflow_id):
        """determine if it's a 384-well plate"""
        final_path = os.path.basename(dir_name)
        parsed_workflow = final_path.split("__")[0][-6:]
        return final_path.startswith("S") and parsed_workflow == workflow_id

    def create_plate_list_384(self, workflow_id, variant, titration=False):
        """
        create a plate list from an workflow_id and variant names

        Parameters
        ----------
        worflow_id: string
            workflow ID as a zero-padded string
        variant: string
            full variant name as entered in the LIMS
        titration: bool
            Whether or not we're creating a titration plate list.
            This determines what plate prefix we're looking for
            as titration plates begin with T and assay plates begin with S.

        Returns
        -------
        List of full paths to plates
        """
        prefix_char = "T" if titration else "S"
        all_subdirs = [i for i in os.listdir(self.input_dir)]
        full_paths = [os.path.join(self.input_dir, i) for i in all_subdirs]
        # filter to just those of the specific workflow_id and variants
        variant_ints = self.database.get_variant_ints_from_name(variant)
        wanted_workflows = []
        for i in full_paths:
            final_path = os.path.basename(i)
            # 384-well plates have the prefix "S01000000"
            if (
                final_path[3:9] == workflow_id
                and final_path[0] == prefix_char
                and int(final_path[1:3]) in variant_ints
            ):
                wanted_workflows.append(i)
        return wanted_workflows

    def get_workflow_id(self, src_path):
        """
        returns workflow id as zero-padded string
        e.g src_path("/some/path/S01000999__2021_01_01
        """
        plate_name = self.get_plate_name(src_path)
        workflow_id = plate_name[-6:]
        return workflow_id

    @staticmethod
    def is_titration_plate(plate_name):
        """
        determines if a plate is a titration plate.
        titrations plates starts with T{variant_ints}{workflow_id}
        """
        return plate_name.startswith("T") and plate_name[1:].isdigit()

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
            # send warning message to slack
            return_code = utils.send_slack_warning(
                f"Detected invalid directory name in NA_raw_data: {plate_dir}"
            )
            if return_code == 200:
                logging.info("sent slack warning")
            else:
                logging.error("failed to send slack warning")
            experiment_name = None
        return experiment_name

    @staticmethod
    def get_plate_name(dir_name):
        """
        get the name of the plate from the full directory path
        e.g get_plate_name("/some/path/S01000999__2021-01-01T00_00_00-Measurement 1")
            => "S01000999"
        """
        plate_dir = os.path.basename(dir_name)
        return plate_dir.split("__")[0]
