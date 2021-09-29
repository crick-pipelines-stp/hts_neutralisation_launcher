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


# first phenix
HARMONY_1_NAME = "1400l18172"
HARMONY_1_IP_ADDRESS = "10.6.58.52"
# second phenix
HARMONY_2_NAME = "2400l21087"
HARMONY_2_IP_ADDRESS = "10.6.48.135"


class ImageStitcher:
    """docstring"""

    def __init__(
        self,
        indexfile_path,
        output_dir="/mnt/proj-c19/ABNEUTRALISATION/stitched_images",
    ):
        self.placeholder_url = "/mnt/proj-c19/ABNEUTRALISATION/placeholder_image.png"
        self.indexfile_path = indexfile_path
        self.indexfile = pd.read_csv(indexfile_path, sep="\t")
        self.indexfile = self.fix_missing_wells(self.indexfile)
        self.output_dir = output_dir
        self.well_dict = WELL_DICT
        self.plate_images = None
        self.dilution_images = None
        self.ch1_max = 800  # DAPI
        self.ch2_max = 1000  # virus

    def fix_missing_wells(self, indexfile):
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
        merged["URL"] = merged["URL"].fillna(self.placeholder_url)
        merged = merged.sort_values(["Row", "Column", "Channel ID"])
        return merged

    @staticmethod
    def fix_url(url):
        """
        vm doesn't find harmony computer by name, replace with ip
        address in URLs
        """
        if url.startswith(f"http://{HARMONY_1_NAME}/"):
            url = url.replace(HARMONY_1_NAME, HARMONY_1_IP_ADDRESS)
        if url.startswith(f"http://{HARMONY_2_NAME}/"):
            url = url.replace(HARMONY_2_NAME, HARMONY_2_IP_ADDRESS)
        return url

    def stitch_plate(self, well_size=(80, 80)):
        """docstring"""
        ch_images = {1: [], 2: []}
        plate_images = dict()
        for channel_name, group in self.indexfile.groupby("Channel ID"):
            for index, row in group.iterrows():
                url = self.fix_url(row["URL"])
                img = skimage.io.imread(url, as_gray=True)
                # TODO add logging
                img = skimage.transform.resize(
                    img, well_size, anti_aliasing=True, preserve_range=True
                )
                ch_images[channel_name].append(img)
            img_stack = np.stack(ch_images[channel_name])
            img_plate = img_stack.reshape(384, *well_size)
            # rescale
            if channel_name == 1:
                img_plate = img_plate / self.ch1_max
            elif channel_name == 2:
                img_plate = img_plate / self.ch2_max
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
            # TODO rescale to some constant value
            plate_images[channel_name] = img_montage
        self.plate_images = plate_images

    def stitch_sample(self, well, img_size=(360, 360)):
        """docstring"""
        df = self.indexfile.copy()
        sample_dict = defaultdict(dict)
        images = []
        # as we're dealing with the 96-well labels, but the indexfile is using
        # the original 384-well labels, we need to get the 4 384-well labels
        # which correspond to the given sample well label
        wells_384 = self.well_dict[well]
        for well_384 in wells_384:
            row, column = utils.well_to_row_col(well_384)
            # subset dataframe to just correct row/columns
            df_subset = df[(df["Row"] == row) & (df["Column"] == column)]
            for channel_name, group in df_subset.groupby("Channel ID"):
                for _, group_row in group.iterrows():
                    dilution = utils.get_dilution_from_row_col(
                        group_row["Row"], group_row["Column"]
                    )
                    url = self.fix_url(group_row["URL"])
                    img = skimage.io.imread(url, as_gray=True)
                    sample_dict[channel_name].update({dilution: img})
        for channel in [1, 2]:
            for dilution in [40, 160, 640, 2560]:
                img = sample_dict[channel][dilution]
                img = skimage.transform.resize(
                    img, img_size, anti_aliasing=True, preserve_range=True
                )
                # rescale image intensities
                if channel == 1:
                    img = img / self.ch1_max
                if channel == 2:
                    img = img / self.ch2_max
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

    def stitch_all_samples(self, img_size=(360, 360)):
        """docstring"""
        dilution_images = {}
        for well in self.well_dict.keys():
            # TODO: add logging
            sample_img = self.stitch_sample(well, img_size)
            dilution_images[well] = sample_img
        self.dilution_images = dilution_images

    def save_all(self):
        self.create_output_dir()
        if self.dilution_images is None:
            raise RuntimeError("no dilution images, have you run stitch_all_samples()?")
        if self.plate_images is None:
            raise RuntimeError("no plate images, have you run stitch_plate()?")
        # TODO: add logging
        for channel_num, plate_arr in self.plate_images.items():
            plate_path = os.path.join(self.output_dir_path, f"plate_{channel_num}.png")
            plate_arr = skimage.img_as_ubyte(plate_arr)
            skimage.io.imsave(fname=plate_path, arr=plate_arr)
        for well_name, well_arr in self.dilution_images.items():
            well_path = os.path.join(self.output_dir_path, f"well_{well_name}.png")
            # TODO: add logging
            well_arr = skimage.img_as_ubyte(well_arr)
            skimage.io.imsave(fname=well_path, arr=well_arr)

    def create_output_dir(self):
        """docstring"""
        plate_barcode = self.get_plate_barcode()
        output_dir_path = os.path.join(self.output_dir, plate_barcode)
        os.makedirs(output_dir_path, exist_ok=True)
        self.output_dir_path = output_dir_path

    def get_plate_barcode(self):
        """get plate barcode from indexfile path"""
        prev_dir = self.indexfile_path.split(os.sep)[-2]
        return prev_dir.split("__")[0]
