import logging
import os
from string import ascii_uppercase
from typing import Optional, Tuple

from . import slack
from .well_dict import well_dict_r

log = logging.getLogger(__name__)


def is_even(n: int) -> bool:
    return n % 2 == 0


def is_odd(n: int) -> bool:
    return n % 2 != 0


def well_to_row_col(well: str) -> Tuple[int, int]:
    """
    convert alphanumeric well label to row and column integers (1-indexed)
    """
    row = ord(well[0].lower()) - 96
    col = int(well[1:])
    return row, col


def get_dilution_from_row_col(row, col) -> int:
    """
    Get dilution value from a well's row and column position.
    """
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


def is_titration_plate(plate_name: str) -> bool:
    """
    determines if a plate is a titration plate.
    titrations plates starts with T{variant_ints}{workflow_id}
    """
    return plate_name.startswith("T") and plate_name[1:].isdigit()


def is_384_well_plate(dir_name: str, workflow_id: str) -> bool:
    basename = os.path.basename(dir_name)
    parsed_workflow = basename.split("__")[0][-6:]
    return basename.startswith(("S", "T")) and parsed_workflow == workflow_id


def get_experiment_name(dir_name: str) -> Optional[str]:
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


def get_plate_name(dir_name: str) -> str:
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


def get_workflow_id(src_path: str) -> str:
    """returns workflow id as zero-padded string"""
    plate_name = get_plate_name(src_path)
    workflow_id = plate_name[-6:]
    return workflow_id


def row_col_to_well(row: int, col: int) -> str:
    """return well label from row and column integers"""
    return f"{ascii_uppercase[row-1]}{col:02}"


def convert_well_384_to_96(well_384: str) -> str:
    """convert 384 well label to a 96 well label"""
    return well_dict_r[well_384]


def dilution_from_well(well: str) -> int:
    """convert well label to dilution integer (1, 2 ,3, 4)"""
    row = ord(well[0]) - 64
    col = int(well[1:])
    if row % 2 == 0 and col % 2 == 0:
        return 4
    if row % 2 == 1 and col % 2 == 0:
        return 3
    if row % 2 == 0 and col % 2 == 1:
        return 2
    if row % 2 == 1 and col % 2 == 1:
        return 1
    raise ValueError("shouldn't reach here")
