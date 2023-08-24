"""
NOTE: these tests won't work outside of the Crick network, they need to be
able to access images via Harmony internal ip addresses, so not github actions.
"""

import os
import shutil
import sys

BASE_DIR = os.path.dirname(__file__)
STITCH_IMAGE_DIR = os.path.join(BASE_DIR, "..", "launcher" "stitch_images")
TEST_DATA_DIR = os.path.join(BASE_DIR, "test_data")
TEST_OUTPUT_DIR = os.path.join(BASE_DIR, "output_dir")
INDEXFILE_PATH = os.path.join(TEST_DATA_DIR, "indexfile.txt")
MISSING_IMG_PATH = os.path.join(TEST_DATA_DIR, "placeholder_image.png")

sys.path.append(os.path.join(BASE_DIR, "launcher"))
from launcher.stitch_images import ImageStitcher


def setup_module():
    print("--- setting up module ---")
    if os.path.exists(TEST_OUTPUT_DIR):
        shutil.rmtree(TEST_OUTPUT_DIR)
    os.makedirs(TEST_OUTPUT_DIR, exist_ok=False)
    stitcher = ImageStitcher(
        indexfile_path=INDEXFILE_PATH,
        output_dir=TEST_OUTPUT_DIR,
        missing_well_img_path=MISSING_IMG_PATH,
    )
    stitcher.stitch_plate()
    stitcher.stitch_all_samples()
    stitcher.save_all()


def test_creates_missing_images():
    plate_output_dir = os.path.join(TEST_OUTPUT_DIR, "test_data")
    assert "well_H12.png" in os.listdir(plate_output_dir)


def test_creates_parent_directory():
    plate_output_dir = os.path.join(TEST_OUTPUT_DIR, "test_data")
    assert os.path.isdir(plate_output_dir)


def test_creates_all_expected_files():
    plate_output_dir = os.path.join(TEST_OUTPUT_DIR, "test_data")
    all_files = os.listdir(plate_output_dir)
    assert len(all_files) == 98
    well_files = [i for i in all_files if i.startswith("well_")]
    assert len(well_files) == 96
    plate_files = [i for i in all_files if i.startswith("plate_")]
    assert len(plate_files) == 2


def teardown_module():
    print("--- tearing down module ---")
    shutil.rmtree(TEST_OUTPUT_DIR)
