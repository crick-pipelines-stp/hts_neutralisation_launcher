import logging
from dispatch import Dispatcher


LOGNAME = "/mnt/proj-c19/ABNEUTRALISATION/analysis_logs/neutralisation_snapshotter.log"


def main():
    dispatch = Dispatcher()
    new_plates = dispatch.get_new_directories()
    for plate in new_plates:
        dispatch.dispatch_plate(plate)


if __name__ == "__main__":
    logging.basicConfig(
        filename=LOGNAME,
        level=logging.INFO,
        format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    )
    main()
