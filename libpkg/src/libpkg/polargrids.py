import math


def ll_from_polar_gridpoint(gridpoint_dict, grid_dict):
    """
    gridpoint_dict is a dictionary containing the gridpoint to convert:
        'i' and 'j' are the gridpoints in polar space

    grid_dict is a dictionary of grid information:
        'ni' and 'nj': the number of gridpoints in the i and j directions
        'proj': projection of the grid - either "S" or "N"
        'tan_lat': tangent latitude
        'dx': x-distance between two points in kilometers at 'tan_lat'
        'orient_elon': longitude of orientation in degrees (0. to 360.)

    ll_dict is the returned dictionary
        'lat' and 'elon' are the latitude and east longitudes of the
            gridpoint in geo space
    """
    x = gridpoint_dict['i'] + 1. - (grid_dict['ni'] + 1.) / 2.
    if grid_dict['proj'] == "S":
        x = -x

    y = gridpoint_dict['j'] + 1. - (grid_dict['nj'] + 1.) / 2.
    if x == 0. and y == 0.:
        ll_dict = {'lat': 90., 'elon': 360.}
    else:
        r2 = (x * x) + (y * y)
        R_EARTH = 6.3712e+6
        re2 = math.pow(
                ((1. + math.sin(math.radians(grid_dict['tan_lat']))) *
                 R_EARTH / (grid_dict['dx'] * 1000.)), 2.)
        ll_dict['lat'] = math.degrees(math.asin((re2 - r2) / (re2 + r2)))
        ll_dict['elon'] = (
                math.degrees(math.atan2(y, x)) + 90. -
                (360. - grid_dict['orient_elon']))

    if grid_dict['proj'] == "S":
        ll_dict['lat'] = -ll_dict['lat']

    while ll_dict['elon'] < 0.:
        ll_dict['elon'] += 360.

    return ll_dict
