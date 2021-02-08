import os
from main import MyEventHandler


def setup_module():
    pass


def teardown_module():
    os.remove("test_db.sqlite")


def test_get_experiment_name_on_384():
    out = MyEventHandler(".", "test_db.sqlite").get_experiment_name("/test/S01000001")
    assert out == "000001"


def test_get_experiment_name_on_96():
    out = MyEventHandler(".", "test_db.sqlite").get_experiment_name("/test/A11000001")
    assert out == "000001"
