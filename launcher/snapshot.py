"""
An filesystem monitoring class as an alternative to Watchdog.
Basically a set using sqlite.
Records the names of directories in a parent directory in a database
alongside a hash, creating a snapshot. This can then be used to determine
if new directories have been exported since the last time it has been run.
This is useful when the CAMP mount is being temperamental, but requires a
cron job.

An example workflow:

    results_dir = "/mnt/proj-c19/ABNEUTRALISATION/NA_raw_data"

    snapshot = Snapshot(results_dir)
    if snapshot.current_hash == snapshot.stored_hash:
        # nothing has changed
        sys.exit(0)

    # get new directory names
    new_data = snapshot.get_new_dirs()

    # record new snapshot
    snapshot.make_snapshot()

    # do stuff with new_data
    ...
"""


import hashlib
import os
from glob import glob
import sqlite3
from typing import List, Optional


class SnapshotDB:
    """An sqlite database which handles dir names and hashes"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = self.create_connection()

    def create_connection(self):
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
        with self.con:
            self.con.execute("INSERT OR IGNORE INTO snapshot(id) VALUES(?)", (new_dir,))

    def rm_dir(self, rm_dir: str):
        with self.con:
            self.con.execute("DELETE FROM snapshot WHERE id = ?", (rm_dir,))

    def is_new_dir(self, dir_name: str) -> bool:
        with self.con:
            for _ in self.con.execute(
                "SELECT 1 FROM snapshot WHERE id = ?", (dir_name,)
            ):
                return False
        return True

    def drop_snapshot(self):
        with self.con:
            self.con.execute("DELETE FROM snapshot")

    def create_snapshot(self, dirnames: List[str]):
        for dirname in dirnames:
            self.add_dir(dirname)

    def add_hash(self, hash_val: str):
        with self.con:
            self.con.execute("UPDATE hash SET VALUE=? WHERE id=1", (hash_val,))
            self.con.execute(
                "INSERT OR IGNORE INTO hash (id, value) VALUES (1, ?)", (hash_val,)
            )

    def get_hash(self) -> Optional[str]:
        cur = self.con.cursor()
        cur.execute("SELECT value FROM hash WHERE id=1")
        val = cur.fetchone()
        cur.close()
        return val[0] if val else None


class Snapshot:
    def __init__(
        self,
        parent_dir: str,
        db_path=".snapshot.db",
        prefix="S",
        suffix="-Measurement 1",
    ):
        self.parent_dir = parent_dir
        self.prefix = prefix
        self.suffix = suffix
        self.db = SnapshotDB(db_path)

    @property
    def current_hash(self) -> str:
        base_filenames = self.get_all_dirnames()
        filenames_utf8 = " ".join(base_filenames).encode("utf-8")
        return hashlib.sha256(filenames_utf8).hexdigest()

    @property
    def stored_hash(self) -> str:
        return self.db.get_hash()

    def get_all_dirnames(self) -> List[str]:
        glob_str = f"{self.prefix}*{self.suffix}"
        filenames = glob(os.path.join(self.parent_dir, glob_str))
        base_filenames = [os.path.basename(i) for i in filenames]
        return sorted(base_filenames)

    def make_snapshot(self, fresh=True):
        dirnames = self.get_all_dirnames()
        if fresh:
            self.db.drop_snapshot()
        self.db.create_snapshot(dirnames)
        self.db.add_hash(self.current_hash)

    def get_new_dirs(self) -> List[str]:
        new_dirs = []
        all_dirs = self.get_all_dirnames()
        for dirname in all_dirs:
            if self.db.is_new_dir(dirname):
                full_dir_path = os.path.join(self.parent_dir, dirname)
                new_dirs.append(full_dir_path)
        return new_dirs
