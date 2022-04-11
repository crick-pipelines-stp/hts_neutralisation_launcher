import logging
from dispatch import Dispatcher
from config import parse_config


config = parse_config()["titration"]
RESULTS_DIR = config["results_dir"]
SNAPSHOT_DB_PATH = config["snapshot_db"]
LOGNAME = config["log_path"]


def main():
    # save again for titration directory
    dispatch_titration = Dispatcher(
        results_dir=RESULTS_DIR,
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
