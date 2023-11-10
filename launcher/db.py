import datetime
import os
from enum import Enum, auto
from typing import List

import models
import slack
import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import or_


class AnalysisState(Enum):
    NEW = auto()
    FINISHED = auto()
    RECENT = auto()
    STALE = auto()


def create_engine(test=False) -> sqlalchemy.Engine:
    """create sqlalchemy engine"""
    user = os.environ.get("NE_USER")
    if test:
        host = os.environ.get("NE_HOST_TEST")
    else:
        host = os.environ.get("NE_HOST_PROD")
    password = os.environ.get("NE_PASSWORD")
    if None in (user, host, password):
        raise EnvironmentError("db credentials not found in users environment")
    engine = sqlalchemy.create_engine(
        f"mysql://{user}:{password}@{host}/serology", pool_pre_ping=True
    )
    return engine


def create_session(engine) -> sqlalchemy.orm.Session:
    """create sqlalchemy ORM session"""
    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    return Session()


class Database:
    """class to interact with the LIMS serology database."""

    def __init__(self, session: sqlalchemy.orm.Session, task_timeout_mins: int = 30):
        self.session = session
        self.task_timeout_mins = task_timeout_mins
        self.task_timeout_sec = task_timeout_mins * 60

    @staticmethod
    def now() -> str:
        return datetime.datetime.utcnow().replace(microsecond=0).isoformat(" ")

    def get_variant_from_plate_name(self, plate_name: str, is_titration=False) -> str:
        """
        plate_name is os.path.basename(full_path).split("__")[0]

        this returns the variant name from the NE_available_strains
        table based on the plate prefix
        """
        plate_prefix = plate_name[:3]
        if is_titration:
            plate_prefix = plate_prefix.replace("T", "S")
        else:
            if not plate_prefix.startswith("S"):
                # plate prefixes with "A" etc.
                plate_prefix = "S" + plate_prefix[1:]
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
            # try without any prefix, variants might be listed in the database
            # by their digits alone without any sample type prefix.
            plate_prefix = plate_prefix[1:]
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
                raise VariantLookupError(
                    f"cannot find variant from plate name {plate_name}"
                )
        return result.mutant_strain

    def get_variant_ints_from_name(self, variant_name: str) -> List[int]:
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

    def get_analysis_state(
        self, workflow_id: str, variant: str, is_titration: bool = False
    ) -> AnalysisState:
        """
        Get the current state of an analysis from the `processed` table.

        1.  This first checks for any row in the `processed` table for the
            given `experiment` and `variant`, if there is no row then the
            experiment has not been processed, and returns "new".
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
            so returning "stale"

        Arguments:
        -----------
            workflow_id: string
            variant: string
            titration: bool
                whether or not this is a titration workflow
        Returns:
        --------
            AnalysisState enum
        """
        if is_titration:
            result = (
                self.session.query(models.Titration)
                .filter(
                    models.Titration.workflow_id == int(workflow_id),
                    models.Titration.variant == variant,
                )
                .first()
            )
        else:  # is analysis
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
            return AnalysisState.NEW
        else:
            # 2. now check for `finished_at` time
            if result.finished_at is not None:
                # we have a finished_at time, it's definitely been processed
                return AnalysisState.FINISHED
            else:
                # `finished_at` is null, look how recent `created_at` timestamp is
                # 3. check how recent `created_at` timestamp is
                time_now = datetime.datetime.utcnow()
                time_difference = (time_now - result.created_at).total_seconds()
                # "recent" defined as within 30 minutes
                is_recent = int(time_difference) < self.task_timeout_sec
                if is_recent:
                    # probably sat in the job-queue, don't re-submit analysis
                    # TODO: check celery job queue for this entry
                    return AnalysisState.RECENT
                else:
                    # 3b. try re-submitting the analysis
                    # (will have to update the created_at time)
                    return AnalysisState.STALE

    def get_stitching_state(self, plate_name: str) -> AnalysisState:
        """docstring"""
        result = (
            self.session.query(models.Stitching)
            .filter(models.Stitching.plate_name == plate_name)
            .first()
        )
        if result is None:
            return AnalysisState.NEW
        if result.finished_at is not None:
            return AnalysisState.FINISHED
        else:
            # see if it's been recently submitted
            time_now = datetime.datetime.utcnow()
            time_difference = (time_now - result.created_at).total_seconds()
            is_recent = int(time_difference) < self.task_timeout_sec
            return AnalysisState.RECENT if is_recent else AnalysisState.STALE

    def is_plate_stitched(self, plate_name: str) -> bool:
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

    def _processed_entry_exists(self, workflow_id: str, variant: str) -> bool:
        """
        Check if a row exists for a given workflow_id/variant in the
        analysis tracking table.
        """
        result = (
            self.session.query(models.Analysis)
            .filter(
                models.Analysis.workflow_id == int(workflow_id),
                models.Analysis.variant == variant,
            )
            .first()
        )
        return result is not None

    def _alert_if_not_exists(self, workflow_id: str, variant: str) -> None:
        """
        Raise error and send slack alert if trying to update an entry, but
        there is no entry found for that workflow_id & variant in the
        analysis tracking table.
        """
        if not self._processed_entry_exists(workflow_id, variant):
            msg = f"no entry found for {workflow_id} {variant} in processed table, cannot update"
            slack.send_simple_alert(
                workflow_id=workflow_id, variant=variant, message=msg
            )
            raise NoWorkflowError(msg)

    def create_analysis_entry(self, workflow_id: str, variant: str) -> None:
        """create entry for new job submission with current timestamp"""
        analysis = models.Analysis(
            workflow_id=int(workflow_id), variant=variant, created_at=self.now()
        )
        self.session.add(analysis)
        self.session.commit()

    def update_analysis_entry(self, workflow_id: str, variant: str) -> None:
        """update created_at time for resubmitting a stale job"""
        self._alert_if_not_exists(workflow_id, variant)
        self.session.query(models.Analysis).filter(
            models.Analysis.workflow_id == int(workflow_id)
        ).filter(models.Analysis.variant == variant).update(
            {models.Analysis.created_at: self.now()}
        )
        self.session.commit()

    def mark_analysis_entry_as_finished(self, workflow_id: str, variant: str) -> None:
        """run on task success, update finished_at time"""
        self._alert_if_not_exists(workflow_id, variant)
        # update `finished_at` value to current timestamp
        self.session.query(models.Analysis).filter(
            models.Analysis.workflow_id == int(workflow_id)
        ).filter(models.Analysis.variant == variant).update(
            {models.Analysis.finished_at: self.now()}
        )
        self.session.commit()

    def update_stitching_entry(self, plate_name: str) -> None:
        """update created_at time for resubmitting a stale job"""
        self.session.query(models.Stitching).filter(
            models.Stitching.plate_name == plate_name
        ).update({models.Stitching.created_at: self.now()})
        self.session.commit()

    def mark_stitching_entry_as_finished(self, plate_name: str) -> None:
        """run on task success, update finished_at time"""
        self.session.query(models.Stitching).filter(
            models.Stitching.plate_name == plate_name
        ).update({models.Stitching.finished_at: self.now()})
        self.session.commit()

    def create_stitching_entry(self, plate_name: str) -> None:
        """add a plate to the stitched database"""
        stitched_plate = models.Stitching(plate_name=plate_name, created_at=self.now())
        self.session.add(stitched_plate)
        self.session.commit()

    def create_titration_entry(self, workflow_id: str, variant: str) -> None:
        """create entry for new job with current timestamp"""
        titration = models.Titration(
            workflow_id=int(workflow_id), variant=variant, created_at=self.now()
        )
        self.session.add(titration)
        self.session.commit()

    def update_titration_entry(self, workflow_id: str, variant: str) -> None:
        """update created_at time for resubmitting a stale job"""
        self.session.query(models.Titration).filter(
            models.Titration.workflow_id == int(workflow_id),
            models.Titration.variant == variant,
        ).update({models.Titration.create_at: self.now()})
        self.session.commit()

    def mark_titration_entry_as_finished(self, workflow_id: str, variant: str) -> None:
        """run on task success, update finished_at time"""
        self.session.query(models.Titration).filter(
            models.Titration.workflow_id == int(workflow_id),
            models.Titration.variant == variant,
        ).update({models.Titration.finished_at: self.now()})
        self.session.commit()


class VariantLookupError(Exception):
    pass


class NoWorkflowError(Exception):
    pass
