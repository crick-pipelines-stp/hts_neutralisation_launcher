import datetime
import sqlite3

import utils


class Database:
    """class docstring"""

    def __init__(self, path):
        self.path = path

    def create(self, force=False):
        """
        create processed experiments database if it doesn't
        already exist
        """
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        if force:
            # create database, overwrite any existing tables
            cursor.executescript(
                """
                CREATE TABLE processed
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    experiment CHAR(10) NOT NULL,
                    variant CHAR(10) NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    finished_at TEXT
                );
                CREATE TABLE stitched
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plate_name CHAR(20) NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        else:
            # don't overwrite any existing tables
            cursor.executescript(
                """
                CREATE TABLE IF NOT EXISTS processed
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    experiment CHAR(10) NOT NULL,
                    variant CHAR(10) NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS stitched
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plate_name CHAR(20) NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        conn.commit()
        cursor.close()

    def get_analysis_state(self, experiment, variant):
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
            experiment: string
            variant: string
        Returns:
        --------
            string, one of:
            ("does not exist", "finished", "recent", "stuck")
        """
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        # 1. check for any row for this workflow_id and variant
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM processed
                WHERE experiment=(?) AND variant=(?)
            )
            """,
            (experiment, variant),
        )
        exists = cursor.fetchone()[0]
        if not exists:
            # no row for the given workflow_id and variant
            cursor.close()
            return "does not exist"
        else:
            # 2. now check for `finished_at` time
            cursor.execute(
                """
                SELECT created_at, finished_at
                FROM processed
                WHERE experiment=(?) AND variant=(?)
                """,
                (experiment, variant),
            )
            created_at, finished_at = cursor.fetchone()
            cursor.close()
            if finished_at is not None:
                # we have a finished_at time, it's definitely been processed
                return "finished"
            else:
                # `finished_at` is null, look how recent `created_at` timestamp is
                # 3. check how recent `created_at` timestamp is
                time_now = datetime.datetime.utcnow()
                created_at_dt = datetime.datetime.fromisoformat(created_at)
                time_difference = (time_now - created_at_dt).total_seconds()
                # "recent" defined as within 30 minutes
                is_recent = int(time_difference) < 60*30
                if is_recent:
                    # probably sat in the job-queue, don't re-submit analysis
                    return "recent"
                else:
                    # 3b. try re-submitting the analysis
                    # (will have to update the created_at time)
                    return "stuck"

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
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM stitched WHERE plate_name=(?))",
            (plate_name,)
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def create_analysis_entry(self, experiment, variant):
        """
        run on task submission

        add an experiment to the processed database setting `created_at`
        with the current timestamp (default behaviour)
        """
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO processed (experiment, variant) VALUES (?, ?);",
            (experiment, variant),
        )
        conn.commit()
        cursor.close()

    def _processed_entry_exists(self, experiment, variant):
        """check if a row exists for a given workflow_id/variant"""
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM processed
                WHERE experiment=(?) AND variant=(?)
            )
            """,
            (experiment, variant),
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def update_analysis_entry(self, experiment, variant):
        """
        run on task re-submission after delay

        for a given workflow_id/variant, replace `created_at` time
        to current timestamp when relaunching a stuck experiment.
        """
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        if not self._processed_entry_exists(experiment, variant):
            cursor.close()
            msg = f"no entry found for {experiment} {variant} in processed table, cannot update"
            utils.send_simple_slack_alert(workflow_id=experiment, variant=variant, message=msg)
            raise RuntimeError(msg)
        # update `created_at` value to current timestamp
        now = datetime.datetime.utcnow().replace(microsecond=0).isoformat(" ")
        cursor.execute(
            """
            UPDATE
                processed
            SET
                created_at=(?)
            WHERE
                experiment=(?) AND variant=(?)
            """,
            (now, experiment, variant),
        )
        conn.commit()
        cursor.close()

    def mark_analysis_entry_as_finished(self, experiment, variant):
        """run on task success"""
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        if not self._processed_entry_exists(experiment, variant):
            cursor.close()
            msg = f"no entry found for {experiment} {variant} in processed table, cannot update"
            utils.send_simple_slack_alert(workflow_id=experiment, variant=variant, message=msg)
            raise RuntimeError(msg)
        # update `finished_at` value to current timestamp
        now = datetime.datetime.utcnow().replace(microsecond=0).isoformat(" ")
        cursor.execute(
            """
            UPDATE
                processed
            SET
                finished_at=(?)
            WHERE 
                experiment=(?) AND variant=(?)
            """,
            (now, experiment, variant)
        )
        conn.commit()
        cursor.close()

    def add_stitched_plate(self, plate_name):
        """add a plate to the stitched database"""
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO stitched (plate_name) VALUES (?);",
            (plate_name,)
        )
        conn.commit()
        cursor.close()
