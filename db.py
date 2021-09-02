import datetime
import os

import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import or_

import models
import utils
from retry import retry


def create_engine(test=False):
    """docstring"""
    user = os.environ.get("NE_USER")
    if test:
        host = os.environ.get("NE_HOST_TEST")
    else:
        host = os.environ.get("NE_HOST_PROD")
    password = os.environ.get("NE_PASSWORD")
    if None in (user, host, password):
        raise KeyError("db credentials not found in users environment")
    engine = sqlalchemy.create_engine(
            f"mysql://{user}:{password}@{host}/serology"
    )
    return engine


def create_session(engine):
    """docstring"""
    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    return Session()


class Database:
    """class docstring"""

    def __init__(self, session):
        self.session = session

    @staticmethod
    def now():
        return datetime.datetime.utcnow().replace(microsecond=0).isoformat(" ")

    @retry(sqlalchemy.exc.OperationalError)
    def get_variant_from_plate_name(self, plate_name):
        """
        plate_name is os.path.basename(full_path).split("__")[0]

        this returns the variant name from the NE_available_strains
        table based on the plate prefix
        """
        plate_prefix = plate_name[:3]
        result = (
            self.session.query(models.Variant)
            .filter(
                or_(
                    models.Variant.plate_id_1 == plate_prefix,
                    models.Variant.plate_id_2 == plate_prefix,
                )
            )
            .first()
        )
        if result is None:
            raise ValueError(
                f"cannot find variant from plate name {plate_name}"
            )
        return result.mutant_strain

    @retry(sqlalchemy.exc.OperationalError)
    def get_variant_ints_from_name(self, variant_name):
        """
        get plate prefix integers from variant name.
        e.g "England2" => [1, 2]
            "B117" => [3, 4]
        """
        result = (
            self.session.query(models.Variant)
            .filter(models.Variant.mutant_strain == variant_name)
            .first()
        )
        return sorted([int(result.plate_id_1[1:]), int(result.plate_id_2[1:])])

    @retry(sqlalchemy.exc.OperationalError)
    def get_analysis_state(self, workflow_id, variant):
        """
        Get the current state of an analysis from the `processed` table.

        1.  This first checks for any row in the `processed` table for the
            given `experiment` and `variant`, if there is no row then the
            experiment has not been processed, and returns "does not exist".
        2.  If there is a row for the current experiment, we then check for
            the presence of a `finished_at` timestamp in the `processed` table,
            if this is present then then experiment has already been analysed,
            and returns "finished".
        3.  If `finished_at` is null, we then look at the `created_at`
            timestamp, if this is recent this experiment is probably still
            running or sat in the work queue, so return "recent" so a duplicate
            analysis is not launched.
        3b. If `created_at` is not recent (unlikely), then something has
            gone wrong and we should re-submit the experiment for analysis,
            so returning "stuck"

        Arguments:
        -----------
            workflow_id: string
            variant: string
        Returns:
        --------
            string, one of:
            ("does not exist", "finished", "recent", "stuck")
        """
        result = (
            self.session.query(models.Analysis)
            .filter(
                models.Analysis.workflow_id == int(workflow_id),
                models.Analysis.variant == variant,
            )
            .first()
        )
        if result is None:
            # no row for the given workflow_id and variant
            return "does not exist"
        else:
            # 2. now check for `finished_at` time
            if result.finished_at is not None:
                # we have a finished_at time, it's definitely been processed
                return "finished"
            else:
                # `finished_at` is null, look how recent `created_at` timestamp is
                # 3. check how recent `created_at` timestamp is
                time_now = datetime.datetime.utcnow()
                time_difference = (time_now - result.created_at).total_seconds()
                # "recent" defined as within 30 minutes
                is_recent = int(time_difference) < 60*30
                if is_recent:
                    # probably sat in the job-queue, don't re-submit analysis
                    return "recent"
                else:
                    # 3b. try re-submitting the analysis
                    # (will have to update the created_at time)
                    return "stuck"

    @retry(sqlalchemy.exc.OperationalError)
    def get_stitching_state(self, plate_name):
        """docstring"""
        result = (
            self.session.query(models.Stitching)
            .filter(models.Stitching.plate_name == plate_name)
            .first()
        )
        if result is None:
            return "does not exist"
        if result.finished_at is not None:
            return "finished"
        else:
            # see if it's been recently submitted
            time_now = datetime.datetime.utcnow()
            time_difference = (time_now - result.created_at).total_seconds()
            is_recent = int(time_difference) < 60*30
            return "recent" if is_recent else "stuck"

    @retry(sqlalchemy.exc.OperationalError)
    def is_plate_stitched(self, plate_name):
        """
        Check if a plate is already stitched.
        Arguments:
        ----------
            plate_name: string
        Returns:
        ---------
            Boolean
        """
        result = (
            self.session.query(models.Stitching)
            .filter(models.Stitching.plate_name == plate_name)
            .first()
        )
        return result is not None

    @retry(sqlalchemy.exc.OperationalError)
    def create_analysis_entry(self, workflow_id, variant):
        """
        run on task submission

        add an experiment to the processed database setting `created_at`
        with the current timestamp (default behaviour)
        """
        analysis = models.Analysis(
            workflow_id=int(workflow_id),
            variant=variant,
            created_at=self.now()
        )
        self.session.add(analysis)
        self.session.commit()

    @retry(sqlalchemy.exc.OperationalError)
    def _processed_entry_exists(self, workflow_id, variant):
        """check if a row exists for a given workflow_id/variant"""
        result = (
            self.session.query(models.Analysis)
            .filter(
                models.Analysis.workflow_id == int(workflow_id),
                models.Analysis.variant == variant
            )
            .first()
        )
        return result is not None

    def _alert_if_not_exists(self, workflow_id, variant):
        if not self._processed_entry_exists(workflow_id, variant):
            msg = f"no entry found for {workflow_id} {variant} in processed table, cannot update"
            utils.send_simple_slack_alert(
                workflow_id=workflow_id, variant=variant, message=msg
            )
            raise RuntimeError(msg)

    @retry(sqlalchemy.exc.OperationalError)
    def update_analysis_entry(self, workflow_id, variant):
        """
        run on task re-submission after delay

        for a given workflow_id/variant, replace `created_at` time
        to current timestamp when relaunching a stuck experiment.
        """
        self._alert_if_not_exists(workflow_id, variant)
        self.session\
            .query(models.Analysis)\
            .filter(models.Analysis.workflow_id == int(workflow_id))\
            .filter(models.Analysis.variant == variant)\
            .update({models.Analysis.created_at: self.now()})
        self.session.commit()

    @retry(sqlalchemy.exc.OperationalError)
    def mark_analysis_entry_as_finished(self, workflow_id, variant):
        """run on task success"""
        self._alert_if_not_exists(workflow_id, variant)
        # update `finished_at` value to current timestamp
        self.session\
            .query(models.Analysis)\
            .filter(models.Analysis.workflow_id == int(workflow_id))\
            .filter(models.Analysis.variant == variant)\
            .update({models.Analysis.finished_at: self.now()})
        self.session.commit()

    @retry(sqlalchemy.exc.OperationalError)
    def update_stitching_entry(self, plate_name):
        self.session\
            .query(models.Stitching)\
            .filter(models.Stitching.plate_name == plate_name)\
            .update({models.Stitching.created_at: self.now()})
        self.session.commit()

    @retry(sqlalchemy.exc.OperationalError)
    def mark_stitching_entry_as_finished(self, plate_name):
        self.session\
            .query(models.Stitching)\
            .filter(models.Stitching.plate_name == plate_name)\
            .update({models.Stitching.finished_at: self.now()})
        self.session.commit()

    @retry(sqlalchemy.exc.OperationalError)
    def create_stitching_entry(self, plate_name):
        """add a plate to the stitched database"""
        stitched_plate = models.Stitching(
            plate_name=plate_name, created_at=self.now()
        )
        self.session.add(stitched_plate)
        self.session.commit()
