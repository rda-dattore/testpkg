import math


R_EARTH = 6.3712e+6


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
        ll_dict = {}
        r2 = (x * x) + (y * y)
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


def ll_from_lambert_conformal_gridpoint(gridpoint_dict, grid_dict):
    """
    gridpoint_dict is a dictionary containing the gridpoint to convert:
        'i' and 'j' are the gridpoints in lambert-conformal space

    grid_dict is a dictionary of grid information:
        'ni' and 'nj': the number of gridpoints in the i and j directions
        'left_lat': latitude of left-most gridpoint farthest from the pole
                    of projection (e.g. lower-left for N, upper-left for S)
        'left_elon': east longitude of left-most gridpoint farthest from the
                     pole of projection (e.g. lower-left for N, upper-left
                     for S)
        'tan_lat': tangent latitude
        'dx': x-distance between two points in kilometers at 'tan_lat'
        'orient_elon': longitude of orientation in degrees (0. to 360.)

    ll_dict is the returned dictionary
        'lat' and 'elon' are the latitude and east longitudes of the
            gridpoint in geo space
    """
    hemi = 1 if grid_dict['tan_lat'] > 0 else -1
    tan_lat = math.radians(grid_dict['tan_lat'])
    an = hemi * math.sin(tan_lat)
    lat1 = math.radians(grid_dict['left_lat'])
    elon1 = math.radians(grid_dict['left_elon'])
    grid_dict['dx'] *= 1000.
    # radius in meters to lower left corner of grid
    rmll = (
            R_EARTH / grid_dict['dx'] * math.pow(math.cos(tan_lat), 1. - an) *
            pow(1. + an, an) *
            pow(math.cos(lat1) / (1. + hemi * math.sin(lat1)), an) / an)
    # find pole point
    arg = an * math.radians(elon1 - grid_dict['orient_elon'])
    pole_i = 1. - hemi * rmll * math.sin(arg)
    pole_j = 1. + rmll * math.cos(arg)
    # radius to the (i, j) point in grid units
    x = gridpoint_dict['i'] + 1 - pole_i
    y = gridpoint_dict['j'] + 1 - pole_j
    r2 = (x * x) + (y * y)
    # check that requested i and j are not out of bounds
    theta = math.pi * (1. - an)
    beta = abs(math.atan2(x, y))
    if beta < theta:
        return {'lat': 99., 'elon': 999.}

    ll_dict = {}
    if r2 == 0.:
        ll_dict['lat'] = hemi * 90.
        ll_dict['elon'] = grid_dict['orient_elon']
    else:
        aninv = 1. / an
        z = (
                math.pow(an / (R_EARTH / grid_dict['dx']), aninv) /
                math.pow(math.cos(tan_lat), (1. - an) * aninv) *
                (1. + an))
        ll_dict['lat'] = (
                hemi *
                math.degrees(math.pi / 2. - 2. *
                             math.atan(z * math.pow(r2, aninv / 2.))))
        ll_dict['elon'] = (
                grid_dict['orient_elon'] +
                math.degrees(math.atan2(hemi * x, -y) / an))
        while ll_dict['elon'] > 360.:
            ll_dict['elon'] -= 360.

    return ll_dict
