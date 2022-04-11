import os
from collections import defaultdict
import itertools

import numpy as np
import pandas as pd
import skimage
import skimage.transform
import skimage.io

import utils
from well_dict import well_dict as WELL_DICT
from config import parse_config, to_int_tup


config = parse_config()
cfg_stitch = config["image_stitching"]


OUTPUT_DIR = cfg_stitch["output_dir"]
MISSING_WELL_IMG = cfg_stitch["missing_well_path"]
HARMONY_NAME_IP_MAP = dict(config["harmony_mappings"])
MAX_INTENSITY_DAPI = cfg_stitch.getint("max_intensity_dapi")
MAX_INTENSITY_ALEXA488 = cfg_stitch.getint("max_intensity_alexa488")
IMG_SIZE_SAMPLE = to_int_tup(cfg_stitch["img_size_sample"])
IMG_SIZE_PLATE_WELL = to_int_tup(cfg_stitch["img_size_plate_well"])
CHANNELS = to_int_tup(cfg_stitch["channels"])
DILUTIONS = to_int_tup(cfg_stitch["dilutions"])


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
    - Images are saved to a directory on CAMP.
    - Image paths are not recorded as they are consistent and can be
      constructed from the metadata such as plate barcode and well position.
    - Raw images are unsigned 16-bit tiffs, stitched images are saved as
      unsigned 8-bit pngs, with values clipped at a maximum to increase
      contrast.
    """

    def __init__(self, indexfile_path, output_dir=OUTPUT_DIR):
        self.indexfile_path = indexfile_path
        indexfile = pd.read_csv(indexfile_path, sep="\t")
        self.indexfile = self.fix_indexfile(indexfile)
        self.output_dir = output_dir
        self.plate_images = None
        self.dilution_images = None

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
        merged["URL"] = merged["URL"].fillna(MISSING_WELL_IMG)
        merged = merged.sort_values(["Row", "Column", "Channel ID"])
        return merged

    @staticmethod
    def fix_urls(df: pd.DataFrame) -> pd.DataFrame:
        # not a regex, but needed for pandas substring replacement
        return df.URL.replace(HARMONY_NAME_IP_MAP, regex=True)

    def fix_indexfile(self, indexfile: pd.DataFrame) -> pd.DataFrame:
        """
        replace missing wells with placeholder image, and replace
        computer names with ip addresses
        """
        indexfile = self.fix_urls(indexfile)
        indexfile = self.fix_missing_wells(indexfile)
        return indexfile

    def stitch_plate(self, well_size=IMG_SIZE_PLATE_WELL):
        """stitch well images into a plate montage"""
        ch_images = {1: [], 2: []}
        plate_images = dict()
        for channel_name, group in self.indexfile.groupby("Channel ID"):
            for _, row in group.iterrows():
                url = row["URL"]
                img = skimage.io.imread(url, as_gray=True)
                img = skimage.transform.resize(
                    img, well_size, anti_aliasing=True, preserve_range=True
                )
                ch_images[channel_name].append(img)
            img_stack = np.stack(ch_images[channel_name])
            img_plate = img_stack.reshape(384, *well_size)
            # rescale
            if channel_name == 1:
                img_plate /= MAX_INTENSITY_DAPI
            elif channel_name == 2:
                img_plate /= MAX_INTENSITY_ALEXA488
            else:
                raise RuntimeError("unrecognised channel")
            img_plate[img_plate > 1.0] = 1.0
            img_plate = skimage.img_as_float(img_plate)
            img_montage = skimage.util.montage(
                img_plate,
                fill=1.0,
                padding_width=3,
                grid_shape=(16, 24),
                rescale_intensity=False,
            )
            plate_images[channel_name] = img_montage
        self.plate_images = plate_images

    def stitch_sample(self, well: str, img_size=IMG_SIZE_SAMPLE) -> np.array:
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
                    url = group_row["URL"]
                    img = skimage.io.imread(url, as_gray=True)
                    sample_dict[channel_name].update({dilution: img})
        for channel in CHANNELS:
            for dilution in DILUTIONS:
                img = sample_dict[channel][dilution]
                img = skimage.transform.resize(
                    img, img_size, anti_aliasing=True, preserve_range=True
                )
                # rescale image intensities
                if channel == 1:
                    img /= MAX_INTENSITY_DAPI
                if channel == 2:
                    img /= MAX_INTENSITY_ALEXA488
                img[img > 1.0] = 1
                img = skimage.img_as_float(img)
                images.append(img)
        img_stack = np.stack(images).reshape(8, *img_size)
        img_montage = skimage.util.montage(
            arr_in=img_stack,
            fill=1.0,  # white if rescale_intensity is True
            grid_shape=(2, 4),
            rescale_intensity=False,
            padding_width=10,
            multichannel=False,
        )
        return img_montage

    def stitch_all_samples(self, img_size=IMG_SIZE_SAMPLE):
        """stitch but don't save sample images"""
        dilution_images = {}
        for well in WELL_DICT.keys():
            sample_img = self.stitch_sample(well, img_size)
            dilution_images[well] = sample_img
        self.dilution_images = dilution_images

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
