import os
import logging

from launcher import slack


log = logging.getLogger(__name__)


def is_even(n):
    return n % 2 == 0


def is_odd(n):
    return n % 2 != 0


def well_to_row_col(well):
    row = ord(well[0].lower()) - 96
    col = int(well[1:])
    return row, col


def get_dilution_from_row_col(row, col):
    row, col = int(row), int(col)
    if is_odd(row) and is_odd(col):
        dilution = 2560
    elif is_odd(row) and is_even(col):
        dilution = 160
    elif is_even(row) and is_odd(col):
        dilution = 640
    elif is_even(row) and is_even(col):
        dilution = 40
    else:
        raise RuntimeError()
    return dilution


def is_titration_plate(plate_name):
    """
    determines if a plate is a titration plate.
    titrations plates starts with T{variant_ints}{workflow_id}
    """
    return plate_name.startswith("T") and plate_name[1:].isdigit()


def is_384_well_plate(dir_name, workflow_id):
    basename = os.path.basename(dir_name)
    parsed_workflow = basename.split("__")[0][-6:]
    return basename.startswith("S") and parsed_workflow == workflow_id


def get_experiment_name(dir_name):
    """get the name of the experiment from a plate directory"""
    plate_dir = os.path.basename(dir_name)
    if plate_dir.startswith("A"):
        experiment_name = plate_dir.split("__")[0][-6:]
    elif plate_dir.startswith("S"):
        # 384-well plates should be the same for now
        experiment_name = plate_dir.split("__")[0][-6:]
    else:
        log.error(f"invalid plate directory name {plate_dir}, skipping")
        # send warning message to slack
        slack.send_warning(
            f"Detected invalid directory name in NA_raw_data: {plate_dir}"
        )
        experiment_name = None
    return experiment_name


def get_plate_name(dir_name):
    """
    get the name of the plate from the full directory path
    e.g
        get_plate_name(
            "/some/path/S01000999__2021-01-01T00_00_00-Measurement 1"
        )
        output: "S01000999"
    """
    plate_dir = os.path.basename(dir_name)
    return plate_dir.split("__")[0]


def get_workflow_id(src_path):
    """returns workflow id as zero-padded string"""
    plate_name = get_plate_name(src_path)
    workflow_id = plate_name[-6:]
    return workflow_id
