import sqlite3


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
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

    def is_experiment_processed(self, experiment, variant):
        """
        check if an experiment is already in the processed database
        Arguments:
        -----------
            experiment: string
            variant: string
        Returns:
        --------
            Boolean
        """
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM processed WHERE experiment=(?) AND variant=(?))",
            (experiment, variant),
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def is_plate_stitched(self, plate_name):
        """
        check if a plate is already in the stitched database
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
            "SELECT EXISTS (SELECT 1 FROM stitched WHERE plate_name=(?))", (plate_name,)
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def add_processed_experiment(self, experiment, variant):
        """add an experiment to the processed database"""
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO processed (experiment, variant) VALUES (?, ?);",
            (experiment, variant),
        )
        conn.commit()
        cursor.close()

    def add_stitched_plate(self, plate_name):
        """add a plate to the stitched database"""
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO stitched (plate_name) VALUES (?);", (plate_name,))
        conn.commit()
        cursor.close()
