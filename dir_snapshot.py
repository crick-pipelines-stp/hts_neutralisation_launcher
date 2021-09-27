"""
An filesystem monitoring class as an alternative to Watchdog.
Records the names of directories in a parent directory in a database
alongside a hash, creating a snapshot. This can then be used to determine
if new directories have been exported since the last time it has been run.
This is useful when the CAMP mount is being tempremental.

An example workflow:

    RESULTS_DIR = "/mnt/proj-c19/NA_raw_data"
    DB_PATH = "snapshot.db"

    snapshot = Snapshot(RESULTS_DIR, DB_PATH)
    if snapshot.current_dir_hash == snapshot.old_dir_hash:
        # nothing has changed
        sys.exit(0)

    # get new directory names
    new_data = snapshot.get_new_dirs()

    # record new snapshot
    snapshot.make_snapshot()
"""


import hashlib
import os
from glob import glob
import sqlite3
from typing import List, Optional


class SnapshotDB:
    """An sqlite database which handles dir names and hashes"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.con = self.create_connection()

    def create_connection(self):
        """create a connection to the snapshot db"""
        con = sqlite3.connect(self.db_path)
        # create db if it doesn't exist
        with con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshot(
                    id PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS hash(
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                """
            )
        return con

    def add_dir(self, new_dir: str):
        """Add directory name to snapshot db"""
        with self.con:
            self.con.execute("INSERT OR IGNORE INTO snapshot(id) VALUES(?)", (new_dir,))

    def rm_dir(self, rm_dir: str):
        """Remove a directory name from the snapshot db"""
        with self.con:
            self.con.execute("DELETE FROM snapshot WHERE id = ?", (rm_dir,))

    def is_new_dir(self, dir_name: str) -> bool:
        """determines if a dir is new by checking if it's in the latest snapshot"""
        with self.con:
            for _ in self.con.execute(
                "SELECT 1 FROM snapshot WHERE id = ?", (dir_name,)
            ):
                return False
        return True

    def create_snapshot(self, dirnames: List[str]):
        """creates a snapshot from a list of directory names"""
        for dirname in dirnames:
            self.add_dir(dirname)

    def add_hash(self, hash_val: str):
        """
        Add hash to database.
        NOTE: It only stores a single value where id=1, so first tries an
              UPDATE, then an INSERT OR IGNORE, seems to work.
        """
        with self.con:
            self.con.execute("UPDATE hash SET VALUE=? WHERE id=1", (hash_val,))
            self.con.execute(
                "INSERT OR IGNORE INTO hash (id, value) VALUES (1, ?)", (hash_val,)
            )

    def get_hash(self) -> Optional[str]:
        """get the hash from the database"""
        cur = self.con.cursor()
        cur.execute("SELECT value FROM hash WHERE id=1")
        val = cur.fetchone()
        cur.close()
        return val[0] if val else None


class Snapshot:
    """
    Snapshot of the exported data directory. Determines if new directories
    have been exported to `base_dir`.
    """

    def __init__(
        self, base_dir: str, snapshot_path: str, prefix="S", suffix="-Measurement 1"
    ):
        self.base_dir = base_dir
        self.prefix = prefix
        self.suffix = suffix
        self.snapshot_db = SnapshotDB(snapshot_path)

    @property
    def current_dir_hash(self) -> str:
        """return a SHA256 hash of the sorted filenames"""
        base_filenames = self.get_all_dirnames()
        filenames_str = " ".join(base_filenames).encode("utf-8")
        return hashlib.sha256(filenames_str).hexdigest()

    @property
    def old_dir_hash(self) -> str:
        """get the dir hash stored in the snapshot db"""
        return self.snapshot_db.get_hash()

    def get_all_dirnames(self) -> List[str]:
        glob_str = f"{self.prefix}*{self.suffix}"
        filenames = glob(os.path.join(self.base_dir, glob_str))
        base_filenames = [os.path.basename(i) for i in filenames]
        assert len(base_filenames) > 0
        base_filenames.sort()
        return base_filenames

    def make_snapshot(self):
        """record a snapshot of all the sub-directory names to disk"""
        dirnames = self.get_all_dirnames()
        self.snapshot_db.create_snapshot(dirnames)
        self.snapshot_db.add_hash(self.current_dir_hash)

    def get_new_dirs(self) -> List[str]:
        """detect new dirs which are not in the latest snapshot"""
        new_dirs = []
        all_dirs = self.get_all_dirnames()
        for dirname in all_dirs:
            if self.snapshot_db.is_new_dir(dirname):
                full_dir_path = os.path.join(self.base_dir, dirname)
                new_dirs.append(full_dir_path)
        return new_dirs
