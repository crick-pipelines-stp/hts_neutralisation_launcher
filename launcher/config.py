from typing import Tuple

from configparser import ConfigParser, ExtendedInterpolation


def parse_config(config_path="./config.ini") -> ConfigParser:
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_path)
    return config


def to_int_tup(x: str) -> Tuple[int]:
    """convert string to tuple of int e.g "1, 2, 3" => (1, 2, 3)"""
    x = x.replace(" ", "")
    return tuple(int(i) for i in x.split(","))
