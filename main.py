import logging
import sys
import time
import argparse

from watchdog.observers.polling import PollingObserver as Observer

from event_handler import MyEventHandler


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "-i", "--input_dir",
        type=str,
        default="/mnt/proj-c19/ABNEUTRALISATION/NA_raw_data/",
        help="path to directory to monitor for new phenix plates"
    )
    arg_parser.add_argument(
        "-l", "--log_file",
        type=str,
        default="/mnt/proj-c19/hts/working/scott/neutralisation_watchdog.log",
        help="where to save the logfile"
    )
    arg_parser.add_argument(
        "--log_level",
        type=str,
        default="info",
        help="logging level, e.g info, warning, debug"
    )
    arg_parser.add_argument(
        "-d",
        "--db_path",
        type=str,
        default="processed_experiments.sqlite",
        help="path to neutralisation launcher database"
    )
    return arg_parser.parse_args()


if __name__ == "__main__":

    args = get_args()

    log_level_dict = {
        "critial": logging.CRITICAL,
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG
    }
    log_level = log_level_dict.get(args.log_level.lower())
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(
                args.log_file, mode="a"
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )

    event_handler = MyEventHandler(args.input_dir, args.db_path)

    observer = Observer()
    observer.schedule(event_handler, args.input_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
