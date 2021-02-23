def is_even(n):
    return n % 2 == 0


def is_odd(n):
    return n % 2 != 0


def well_to_row_col(well):
    row = ord(well[0].lower()) - 96
    col = int(well[1:])
    return row, col


def get_dilution_from_row_col(row, col):
    row, col = int(row), int(col)
    if is_odd(row) and is_odd(col):
        dilution = 2560
    elif is_odd(row) and is_even(col):
        dilution = 160
    elif is_even(row) and is_odd(col):
        dilution = 640
    elif is_even(row) and is_even(col):
        dilution = 40
    else:
        raise RuntimeError()
    return dilution
