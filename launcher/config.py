import os
from typing import Tuple

from configparser import ConfigParser, ExtendedInterpolation


def parse_config(config_path=None) -> ConfigParser:
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "config.ini")
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_path)
    return config


def to_int_tup(x: str) -> Tuple[int]:
    """convert string to tuple of int e.g "1, 2, 3" => (1, 2, 3)"""
    x = x.replace(" ", "")
    return tuple(int(i) for i in x.split(","))
