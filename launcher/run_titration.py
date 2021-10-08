import logging
from dispatch import Dispatcher


LOGNAME = "/mnt/proj-c19/ABNEUTRALISATION/analysis_logs/neutralisation_titration_snapshotter.log"
SNAPSHOT_DB_PATH = "/home/warchas/.snapshot_titration.db"

def main():
    # save again for titration directory
    dispatch_titration = Dispatcher(
        results_dir="/mnt/proj-c19/ABNEUTRALISATION/Titration_raw_data",
        db_path=SNAPSHOT_DB_PATH,
        titration=True
    )
    new_titration_plates = dispatch_titration.get_new_directories()
    for titration_plate in new_titration_plates:
        dispatch_titration.dispatch_plate(titration_plate)


if __name__ == "__main__":
    logging.basicConfig(
        filename=LOGNAME,
        level=logging.INFO,
        format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    )
    main()
