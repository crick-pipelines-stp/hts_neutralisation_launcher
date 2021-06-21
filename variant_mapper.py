import math
import string
from collections import defaultdict


class VariantMapper:
    """
    This class is for handling the conversion between plate barcodes
    and variant letters.

    Plate barcodes contain the variant information in the form of a
    2 digit number. These numbers are sequential pairs which are the
    two replicates associated with the same variant. i.e [01, 02] are
    associated with variant "a", [03, 04] for variant "b" etc.
    The two-digits are the 2nd and 3rd character in the plate barcode.
    e.g "S03000001" has the variant integer 03.

    The neutralisation launcher records variants in the form of a single
    lower case letter. These letters are simply in alphabetical order
    and have no relation to the greek letters assigned to variants by the
    WHO. They are to simplify storing which variants have been analysed per
    workflow ID without having to use the Serology LIMS database.

    The actual variant names can be found in the LIMS serology database in
    the `NE_available_strains` table. Which maps the variant name with
    the pairs of plate integers.

    Overall there is a mapping of:
        `variant_letter` <=> `plate_integer` <=> `variant_name`
              `a`        <=>      `01`       <=>   `England2`
    """

    def __init__(self):
        self.variant_dict = self._create_variant_dict()
        self.variant_dict_rev = self._create_reversed_variant_dict()

    def _create_variant_dict(self):
        """
        Create variant mapping dictionary, to map the paired sequential
        numbers to a variant letter.

        i. e:
            1, 2 => "a"
            3, 4 => "b"

        The dictionary looks like:
            {
                1: a,
                2: a,
                3: b,
                4: b,
                5: c,
                ...
                26: z
            }

        NOTE that at the moment this only goes up to z, so 26 different
        variants, although it can possibly reach 49. We will need to figure
        out how to handle 27+ if we ever reach that far (hopefully not).
        """
        variant_dict = dict()
        for i in range(1, 27):
            letter_int = math.ceil(i / 2) - 1
            variant_dict[i] = string.ascii_lowercase[letter_int]
        return variant_dict

    def _create_reversed_variant_dict(self):
        """
        Reverse the variant dictionary so we can query variant letters
        and return the pair of 2-digit variant integers.
        """
        variant_dict_rev = defaultdict(list)
        for integer, letter in self.variant_dict.items():
            variant_dict_rev[letter].append(integer)
        return variant_dict_rev

    def get_variant_letter(self, plate_name):
        """get variant letter from plate name"""
        variant_int = int(plate_name[1:3])
        if variant_int > 26:
            raise NotImplementedError(
                "MyEventHandler's variant mapping only handles variant numbers "
                + "up to 26. You will need to alter this to use high numbers"
            )
        return self.variant_dict[variant_int]

    def get_variant_ints_from_letter(self, letter):
        """
        get variant integers from variant letter

        e.g
            >>> get_variant_ints_from_letter("a")
            [1, 2]
        """
        return self.variant_dict_rev[letter]
