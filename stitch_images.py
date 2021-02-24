import os
import json
from collections import defaultdict

import numpy as np
import pandas as pd
import skimage
import skimage.transform
import skimage.io

import utils


class ImageStitcher:
    """docstring"""
    def __init__(self, indexfile_path, output_dir="/camp/ABNEUTRALISATION/stitched_images"):
        self.indexfile_path = indexfile_path
        self.indexfile = pd.read_csv(indexfile_path, sep="\t")
        self.output_dir = output_dir
        self.well_dict = json.load(open("well_dict.json"))
        self.plate_images = None
        self.dilution_images = None
        self.ch1_max = 2000 # just a guess
        self.ch2_max = 2000 # just a guess

    def stitch_plate(self, well_size=(80, 80)):
        """docstring"""
        ch_images = {1: [], 2: []}
        plate_images = dict()
        for channel_name, group in self.indexfile.groupby("Channel ID"):
            for index, row in group.iterrows():
                img = skimage.io.imread(row["URL"])
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
            print(img_plate)
            img_plate = skimage.img_as_float(img_plate)
            img_montage = skimage.util.montage(
                img_plate,
                fill=0,
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
                    img = skimage.io.imread(group_row["URL"])
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
            rescale_intensity=False,  # rescales each image seperately between 0 and 1
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
        output_dir_path = self.create_output_dir()
        if self.dilution_images is None:
            raise RuntimeError(
                "no dilution images, have you run stitch_all_samples()?"
            )
        if self.plate_images is None:
            raise RuntimeError(
                "no plate images, have you run stitch_plate()?"
            )
        # TODO: add logging
        for channel_num, plate_arr in self.plate_images.items():
            plate_path = os.path.join(
                self.output_dir_path,
                f"plate_{channel_num}.png"
            )
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
