import os
from event_handler import MyEventHandler


def setup_module():
    pass


def teardown_module():
    os.remove("test_db.sqlite")


def test_get_experiment_name_on_384():
    out = MyEventHandler(".", "test_db.sqlite").get_experiment_name("/test/S01000001")
    assert out == "000001"


def test_get_plate_name():
    out = MyEventHandler(".", "test_db.sqlite").get_plate_name("/home/test/S01000001")
    assert out == "S01000001"


def test_get_variant_letter():
    out = MyEventHandler(".", "test_db.sqlite").get_variant_letter("S01000001")
    assert out == "a"
    out = MyEventHandler(".", "test_db.sqlite").get_variant_letter("S02000001")
    assert out == "a"
    out = MyEventHandler(".", "test_db.sqlite").get_variant_letter("S03000001")
    assert out == "b"
    out = MyEventHandler(".", "test_db.sqlite").get_variant_letter("S04000001")
    assert out == "b"
    out = MyEventHandler(".", "test_db.sqlite").get_variant_letter("S05000001")
    assert out == "c"
    out = MyEventHandler(".", "test_db.sqlite").get_variant_letter("S06000001")
    assert out == "c"

