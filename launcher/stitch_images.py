import itertools
import os
import urllib.error
from collections import defaultdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import skimage
import skimage.io
import skimage.transform

from . import utils
from .config import parse_config, to_int_tup
from .well_dict import well_dict as WELL_DICT

cfg = parse_config()
cfg_stitch = cfg["image_stitching"]


OUTPUT_DIR = cfg_stitch["output_dir"]
MISSING_WELL_IMG = cfg_stitch["missing_well_path"]
HARMONY_NAME_IP_MAP = dict(cfg["harmony_mappings"])
MAX_INTENSITY_DAPI = cfg_stitch.getint("max_intensity_dapi")
MAX_INTENSITY_ALEXA488 = cfg_stitch.getint("max_intensity_alexa488")
IMG_SIZE_SAMPLE = to_int_tup(cfg_stitch["img_size_sample"])
IMG_SIZE_PLATE_WELL = to_int_tup(cfg_stitch["img_size_plate_well"])
CHANNELS = to_int_tup(cfg_stitch["channels"])
DILUTIONS = to_int_tup(cfg_stitch["dilutions"])
PLATE_DIMS = (16, 24)
SAMPLE_DIMS = (2, 4)


class ImageStitcher:
    """
    Image stitching class, for both sample and whole plate images.
    - Channels are stitched and saved as separate images, everything is
      grayscale.
    - Uses the Phenix indexfile to fetch images from a URL.
    - Images are a single field per well which simplifies things.
    - Sometimes certain wells fail to image, these are then missing as rows
      in the indexfile. These are replaced by a placeholder image to show as
      missing and keep plates & samples consistent dimensions.
    - Images are saved to a directory on Nemo.
    - Image paths are not recorded as they are consistent and can be
      constructed from the metadata such as plate barcode and well position.
    - Raw images are unsigned 16-bit tiffs, stitched images are saved as
      unsigned 8-bit pngs, with values clipped at a maximum to increase
      contrast.
    """

    def __init__(
        self,
        indexfile_path: str,
        output_dir: str = OUTPUT_DIR,
        harmony_name_map: Dict = HARMONY_NAME_IP_MAP,
        max_dapi: int = MAX_INTENSITY_DAPI,
        max_alexa488: int = MAX_INTENSITY_ALEXA488,
        missing_well_img_path: str = MISSING_WELL_IMG,
        img_size_sample: Tuple[int] = IMG_SIZE_SAMPLE,
        img_size_plate_well: Tuple[int] = IMG_SIZE_PLATE_WELL,
    ):
        self.indexfile_path = indexfile_path
        self.missing_well_img_path = missing_well_img_path
        self.harmony_name_map = harmony_name_map
        indexfile = pd.read_csv(indexfile_path, sep="\t")
        self.indexfile = self.fix_indexfile(indexfile)
        self.output_dir = output_dir
        self.plate_images = None
        self.dilution_images = None
        self.max_intensity_channel = {1: max_dapi, 2: max_alexa488}
        self.img_size_sample = img_size_sample
        self.img_size_plate_well = img_size_plate_well
        # these are present in the indexfile, can't be loaded
        self.missing_images = []

    def fix_missing_wells(self, indexfile: pd.DataFrame) -> pd.DataFrame:
        """
        find missing wells in the indexfile and add
        them in with the URL pointing to a placeholder
        """
        n_expected_rows = 768  # 384 wells * 2 channels
        if indexfile.shape[0] == n_expected_rows:
            # no missing wells, just return the indexfile as it is
            return indexfile
        # create a dataframe with complete "Row", "Column", "Channel ID" column values
        rows, cols = zip(*itertools.product(range(1, 17), range(1, 25)))
        temp_df_1 = pd.DataFrame({"Row": rows, "Column": cols, "Channel ID": 1})
        temp_df_2 = pd.DataFrame({"Row": rows, "Column": cols, "Channel ID": 2})
        temp_df = pd.concat([temp_df_1, temp_df_2]).sort_values(
            ["Row", "Column", "Channel ID"]
        )
        merged = indexfile.merge(temp_df, how="outer")
        assert merged.shape[0] == n_expected_rows
        # replace missing URLs with the placeholder URL
        merged["URL"] = merged["URL"].fillna(self.missing_well_img_path)
        merged = merged.sort_values(["Row", "Column", "Channel ID"])
        return merged

    def fix_urls(self, df: pd.DataFrame) -> pd.DataFrame:
        # not a regex, but needed for pandas substring replacement
        df.URL = df.URL.replace(self.harmony_name_map, regex=True)
        return df

    def fix_indexfile(self, indexfile: pd.DataFrame) -> pd.DataFrame:
        """
        replace missing wells with placeholder image, and replace
        computer names with ip addresses
        """
        indexfile = self.fix_urls(indexfile)
        indexfile = self.fix_missing_wells(indexfile)
        return indexfile

    def stitch_plate(self) -> None:
        """stitch well images into a plate montage"""
        ch_images = defaultdict(list)
        plate_images = dict()
        for channel, group in self.indexfile.groupby("Channel ID"):
            for _, row in group.iterrows():
                img = self.load_img(row)
                img = skimage.transform.resize(
                    img,
                    self.img_size_plate_well,
                    anti_aliasing=True,
                    preserve_range=True,
                )
                ch_images[channel].append(img)
            img_stack = np.stack(ch_images[channel])
            img_plate = img_stack.reshape(384, *self.img_size_plate_well)
            # rescale intensity
            img_plate /= self.max_intensity_channel[channel]
            img_plate[img_plate > 1.0] = 1.0
            img_plate = skimage.img_as_float(img_plate)
            img_montage = skimage.util.montage(
                img_plate,
                fill=1.0,
                padding_width=3,
                grid_shape=PLATE_DIMS,
                rescale_intensity=False,
            )
            plate_images[channel] = img_montage
        self.plate_images = plate_images

    def stitch_sample(self, well: str) -> np.ndarray:
        """stitch individual sample"""
        df = self.indexfile.copy()
        sample_dict = defaultdict(dict)
        images = []
        # as we're dealing with the 96-well labels, but the indexfile is using
        # the original 384-well labels, we need to get the 4 384-well labels
        # which correspond to the given sample well label
        wells_384 = WELL_DICT[well]
        for well_384 in wells_384:
            row, column = utils.well_to_row_col(well_384)
            # subset dataframe to just correct row/columns
            df_subset = df[(df["Row"] == row) & (df["Column"] == column)]
            for channel_name, group in df_subset.groupby("Channel ID"):
                for _, group_row in group.iterrows():
                    dilution = utils.get_dilution_from_row_col(
                        group_row["Row"], group_row["Column"]
                    )
                    img = self.load_img(group_row)
                    sample_dict[channel_name].update({dilution: img})
        for channel in CHANNELS:
            for dilution in DILUTIONS:
                img = sample_dict[channel][dilution]
                img = skimage.transform.resize(
                    img, self.img_size_sample, anti_aliasing=True, preserve_range=True
                )
                # rescale image intensities
                img /= self.max_intensity_channel[channel]
                img[img > 1.0] = 1
                img = skimage.img_as_float(img)
                images.append(img)
        img_stack = np.stack(images).reshape(8, *self.img_size_sample)
        img_montage = skimage.util.montage(
            arr_in=img_stack,
            fill=1.0,  # white if rescale_intensity is True
            grid_shape=SAMPLE_DIMS,
            rescale_intensity=False,
            padding_width=10,
        )
        return img_montage

    def stitch_all_samples(self):
        """stitch but don't save sample images"""
        dilution_images = {}
        for well in WELL_DICT.keys():
            sample_img = self.stitch_sample(well)
            dilution_images[well] = sample_img
        self.dilution_images = dilution_images

    def create_img_store(self) -> None:
        """
        This loads all images from an indexfile, and stores the resized
        and intensity-scaled images in a dictionary. The images are stored
        twice for the plate images and the sample images, as they require
        different sizes for each.
        The image store is stored in the class as `self.img_store`.
        ---
        img_store:
        {
            "sample": {
                "A01": {
                    1: {1: np.ndarray, 2: np.ndarray, 3:np.ndarray, 4:np.ndarray},
                    2: {1: np.ndarray, 2: np.ndarray, 3:np.ndarray, 4:np.ndarray},
                },
                ...
                "H12": {
                    1: {1: np.ndarray, 2: np.ndarray, 3:np.ndarray, 4:np.ndarray},
                    2: {1: np.ndarray, 2: np.ndarray, 3:np.ndarray, 4:np.ndarray},
                },
            },
            "plate": {1: list[np.ndarray], 2: list[np.ndarray]}
        }
        """
        sample_dict = defaultdict(lambda: defaultdict(dict))
        plate_dict = defaultdict(list)
        for _, row in self.indexfile.iterrows():
            img = self.load_img(row)
            well_384 = utils.row_col_to_well(int(row["Row"]), int(row["Column"]))
            dilution = utils.dilution_from_well(well_384)
            well_96 = utils.convert_well_384_to_96(well_384)
            channel = int(row["Channel ID"])
            img = self.rescale_intensity(img, channel)
            img_resized_plate_well = skimage.transform.resize(
                img, self.img_size_plate_well, anti_aliasing=True, preserve_range=True
            )
            img_resized_sample = skimage.transform.resize(
                img, self.img_size_sample, anti_aliasing=True, preserve_range=True
            )
            sample_dict[well_96][channel][dilution] = img_resized_sample
            plate_dict[channel].append(img_resized_plate_well)
        self.img_store = {"sample": sample_dict, "plate": plate_dict}

    def load_img(self, row: pd.Series):
        """
        Load image from indexfile row.
        If the image is missing then load the placeholder image and add
        row to self.missing_images.
        """
        try:
            img = skimage.io.imread(row["URL"], as_gray=True)
        except (urllib.error.HTTPError, OSError):
            self.missing_images.append(row)
            img = skimage.io.imread(self.missing_well_img_path, as_gray=True)
        return img

    def rescale_intensity(self, img: np.ndarray, channel: int) -> np.ndarray:
        """rescale image intensity, clip values to 1 over this limit"""
        img = img.astype(np.float64)
        img /= self.max_intensity_channel[channel]
        img[img > 1.0] = 1.0
        img = skimage.img_as_float(img)
        return img

    def stitch_and_save_plates(self):
        # stitch and save plates images
        for channel_num in CHANNELS:
            img_stack_plate = np.stack(self.img_store["plate"][channel_num])
            img_montage_plate = skimage.util.montage(
                img_stack_plate,
                fill=1.0,
                padding_width=3,
                grid_shape=PLATE_DIMS,
                rescale_intensity=False,
            )
            plate_path = os.path.join(self.output_dir_path, f"plate_{channel_num}.png")
            plate_arr = skimage.img_as_ubyte(img_montage_plate)
            skimage.io.imsave(fname=plate_path, arr=plate_arr)

    def stitch_and_save_samples(self):
        # stitch and save sample images
        for well in WELL_DICT.keys():
            sample_well = self.img_store["sample"][well]
            sample_imgs = []
            for channel in CHANNELS:
                for dilution in [1, 2, 3, 4]:
                    img = sample_well[channel][dilution]
                    sample_imgs.append(img)
            sample_stack = np.stack(sample_imgs)
            sample_montage = skimage.util.montage(
                arr_in=sample_stack,
                fill=1.0,  # white if rescale_intensity is True
                grid_shape=SAMPLE_DIMS,
                rescale_intensity=False,
                padding_width=10,
            )
            sample_montage = skimage.img_as_ubyte(sample_montage)
            well_path = os.path.join(self.output_dir_path, f"well_{well}.png")
            skimage.io.imsave(fname=well_path, arr=sample_montage)

    def stitch_and_save_all_samples_and_plates(self):
        """
        Stitch all samples and build up whole 384-well plate image as we go,
        this saves loading each image from Harmony twice per standard
        workflow.
        This saves the stitched images immediately after they are stitched
        rather than storing them in `self.dilution_images` and
        `self.plate_images` to reduce memory usage.
        """
        self.create_output_dir()
        self.create_img_store()
        self.stitch_and_save_plates()
        self.stitch_and_save_samples()

    def save_plates(self):
        """save stitched plates"""
        self.create_output_dir()
        if self.plate_images is None:
            raise RuntimeError("no plate images, have you run stitch_plate()?")
        for channel_num, plate_arr in self.plate_images.items():
            plate_path = os.path.join(self.output_dir_path, f"plate_{channel_num}.png")
            plate_arr = skimage.img_as_ubyte(plate_arr)
            skimage.io.imsave(fname=plate_path, arr=plate_arr)

    def save_all(self):
        """save both stitched plate and sample images"""
        self.create_output_dir()
        if self.dilution_images is None:
            raise RuntimeError("no dilution images, have you run stitch_all_samples()?")
        if self.plate_images is None:
            raise RuntimeError("no plate images, have you run stitch_plate()?")
        for channel_num, plate_arr in self.plate_images.items():
            plate_path = os.path.join(self.output_dir_path, f"plate_{channel_num}.png")
            plate_arr = skimage.img_as_ubyte(plate_arr)
            skimage.io.imsave(fname=plate_path, arr=plate_arr)
        for well_name, well_arr in self.dilution_images.items():
            well_path = os.path.join(self.output_dir_path, f"well_{well_name}.png")
            well_arr = skimage.img_as_ubyte(well_arr)
            skimage.io.imsave(fname=well_path, arr=well_arr)

    def create_output_dir(self):
        """create output directory if it doesn't already exist"""
        plate_barcode = self.get_plate_barcode()
        output_dir_path = os.path.join(self.output_dir, plate_barcode)
        os.makedirs(output_dir_path, exist_ok=True)
        self.output_dir_path = output_dir_path

    def get_plate_barcode(self) -> str:
        """get plate barcode from indexfile path"""
        prev_dir = self.indexfile_path.split(os.sep)[-2]
        return prev_dir.split("__")[0]

    def collect_missing_images(self) -> List[str]:
        missing = set()
        for i in self.missing_images:
            name = f"r{i['Row']}c{i['Column']} {i['Channel Name']}"
            missing.add(name)
        return sorted(list(missing))
