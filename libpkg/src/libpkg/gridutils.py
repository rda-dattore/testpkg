import math
import re

from .gridpoints import (
        ll_from_lambert_conformal_gridpoint,
        ll_from_polar_gridpoint)


def decode_latitude(lats):
    lat = float(lats[0:-1])
    if lats[-1] == 'S':
        lat = -lat

    return lat


def decode_elongitude(lons):
    lon = float(lons[0:-1])
    if lons[-1] == 'W':
        lon = -lon + 360.

    return lon


def fill_spatial_domain_from_lambert_conformal_grid(def_params):
    domain = {'slat': 99., 'nlat': -99., 'wlon': 999., 'elon': -999.}
    ni = int(def_params[0])
    nj = int(def_params[1])
    for j in range(0, nj):
        straddles_prime_meridian = False
        previous_elon = None
        for i in range(0, ni):
            ll = ll_from_lambert_conformal_gridpoint(
                    {'i': i, 'j': j},
                    {'left_lat': decode_latitude(def_params[2]),
                     'left_elon': decode_elongitude(def_params[3]),
                     'tan_lat': decode_latitude(def_params[4]),
                     'orient_elon': decode_elongitude(def_params[5]),
                     'dx': float(def_params[7])})
            if ll['lat'] != 99.:
                if previous_elon is None:
                    previous_elon = ll['elon']

                if ll['elon'] < 180. and previous_elon > 180.:
                    straddles_prime_meridian = True

                if straddles_prime_meridian:
                    ll['elon'] += 360.

                domain['slat'] = min(ll['lat'], domain['slat'])
                domain['nlat'] = max(ll['lat'], domain['nlat'])
                domain['wlon'] = min(ll['elon'], domain['wlon'])
                domain['elon'] = max(ll['elon'], domain['elon'])
                previous_elon = ll['elon']

    if domain['slat'] != 99.:
        if domain['wlon'] > 180.:
            domain['wlon'] -= 360.

        if domain['elon'] > 180.:
            domain['elon'] -= 360.

    return domain


def fill_spatial_domain_from_polar_stereographic_grid(def_params):
    domain = {'slat': 99., 'nlat': -99., 'wlon': 999., 'elon': -999.}
    ni = int(def_params[0])
    nj = int(def_params[1])
    start_lat = decode_latitude(def_params[2])
    start_elon = decode_elongitude(def_params[3])
    tan_lat = decode_latitude(def_params[4])
    orient_elon = decode_elongitude(def_params[5])
    dx = float(def_params[7])
    ll = ll_from_polar_gridpoint({'i': 0, 'j': 0},
                                 {'ni': ni, 'nj': nj,
                                  'projection': def_params[6],
                                  'tan_lat': tan_lat, 'dx': dx,
                                  'orient_elon': orient_elon})
    if (abs(ll['lat'] - start_lat) > 0.5 or
            abs(ll['elon'] - start_elon) > 0.5):
        yoverx = math.tan(math.radians(start_elon + 270. - orient_elon))
        if def_params[6] == "S":
            yoverx = -yoverx

        pole_x = 1
        deg_res = dx / (math.cos(math.radians(tan_lat)) * 111.1)
        max_pole_x = int(360. / deg_res) + 1
        pole_y = 0
        while pole_x < max_pole_x:
            ni = pole_x * 2 - 1
            pole_y = round(1 - yoverx * (1 - pole_x))
            nj = pole_y * 2 - 1
            ll = ll_from_polar_gridpoint({'i': 0, 'j': 0},
                                         {'ni': ni, 'nj': nj,
                                          'projection': def_params[6],
                                          'tan_lat': tan_lat, 'dx': dx,
                                          'orient_elon': orient_elon})
            if (abs(ll['lat'] - start_lat) < 0.5 and
                    abs(ll['elon'] - start_elon) < 0.5):
                break

            pole_x += 1

        if pole_x == max_pole_x:
            return domain

    for j in range(0, nj):
        for i in range(0, ni):
            ll = ll_from_polar_gridpoint({'i': i, 'j': j},
                                         {'ni': ni, 'nj': nj,
                                          'projection': def_params[6],
                                          'tan_lat': tan_lat, 'dx': dx,
                                          'orient_elon': orient_elon})
            domain['slat'] = min(ll['lat'], domain['slat'])
            domain['nlat'] = max(ll['lat'], domain['nlat'])
            domain['wlon'] = min(ll['elon'], domain['wlon'])
            domain['elon'] = max(ll['elon'], domain['elon'])

    ll = ll_from_polar_gridpoint({'i': int(ni/2.), 'j': int(nj/2.)},
                                 {'ni': ni, 'nj': nj,
                                  'projection': def_params[6],
                                  'tan_lat': tan_lat, 'dx': dx,
                                  'orient_elon': orient_elon})
    if ((ll['elon'] == 360. and abs(ll['lat']) == 90.) or
            (domain['wlon'] == 0. and domain['elon'] > 359.9)):
        domain['wlon'] = -180.
        domain['elon'] = 180.
    else:
        if domain['wlon'] > 180.:
            domain['wlon'] -= 360.

        if domain['elon'] > 180.:
            domain['elon'] -= 360.

    return domain


def spatial_domain_from_grid_definition(gdef, **kwargs):
    domain = {'wlon': None, 'slat': None, 'elon': None, 'nlat': None}
    def_params = gdef[1].split(":")
    if re.compile("^(latLon|gaussLatLon|mercator)(Cell){0,1}$").match(gdef[0]):
        start_lat = decode_latitude(def_params[2])
        end_lat = decode_latitude(def_params[4])
        domain['slat'] = min(start_lat, end_lat)
        domain['nlat'] = max(start_lat, end_lat)
        start_elon = decode_elongitude(def_params[3])
        end_elon = decode_elongitude(def_params[5])
        xspace = float(def_params[0])
        xres = float(def_params[6])
        if xspace == 0.:
            # reduced grids
            xspace = (end_elon - start_elon) / xres
        else:
            xspace -= 1.
            if xspace == 0.:
                # zonal or global mean
                xspace = end_elon - start_elon
                if xspace == 0.:
                    xspace = 1.
                    xres = 0.
                else:
                    xres = 1.

        if (abs((end_elon - start_elon) / xspace - xres) < 0.01):
            scans_east = True
        elif abs((end_elon + 360. - start_elon) / xspace - xres) < 0.01:
            end_elon += 360.
            scans_east = True
        elif abs((start_elon - end_elon) / xspace - xres) < 0.01:
            scans_east = False
        elif abs((start_elon + 360. - end_elon) / xspace - xres) < 0.01:
            start_elon += 360.
            scans_east = False
        else:
            return domain

        # adjust global grids where boundary is not repeated
        is_global_lon = False
        if abs(end_elon - start_elon) > 0.001:
            if abs(abs(end_elon - start_elon) - 360.) < 0.001:
                is_global_lon = True
            else:
                if end_elon > start_elon:
                    if abs(abs(end_elon + xres - start_elon) - 360.) < 0.001:
                        is_global_lon = True
                else:
                    if abs(abs(end_elon - xres - start_elon) - 360.) < 0.001:
                        is_global_lon = True

        if (abs(abs(domain['nlat'] - domain['slat'] +
                float(def_params[7])) - 180.) < 0.001):
            domain['slat'] = -90.
            domain['nlat'] = 90.

        if is_global_lon:
            if 'centerOn' in kwargs:
                if kwargs['centerOn'] == "dateLine":
                    domain['wlon'] = 0.
                    domain['elon'] = 360.
                elif kwargs['centerOn'] == "primeMeridian":
                    domain['wlon'] = -180.
                    domain['elon'] = 180.

        elif 'centerOn' in kwargs and kwargs['centerOn'] == "dateLine":
            if scans_east:
                domain['wlon'] = start_elon
                domain['elon'] = end_elon
            else:
                domain['wlon'] = end_elon
                domain['elon'] = start_elon

        elif 'centerOn' in kwargs and kwargs['centerOn'] == "primeMeridian":
            if scans_east:
                domain['wlon'] = ((start_elon - 360.) if start_elon >= 180.
                                  else start_elon)
                domain['elon'] = ((end_elon - 360.) if end_elon > 180. else
                                  end_elon)
            else:
                domain['wlon'] = ((end_elon - 360.) if end_elon >= 180. else
                                  end_elon)
                domain['elon'] = ((start_elon - 360.) if start_elon > 180.
                                  else start_elon)

    elif gdef[0].find("lambertConformal") == 0:
        domain = fill_spatial_domain_from_lambert_conformal_grid(def_params)
    elif gdef[0].find("polarStereographic") == 0:
        domain = fill_spatial_domain_from_polar_stereographic_grid(def_params)

    return domain


def convert_grid_definition(gdef):
    grid_data = gdef[1].split(":")
    if gdef[0] == "gaussLatLon":
        slat = float(grid_data[2][0:-1])
        elat = float(grid_data[4][0:-1])
        if ((grid_data[2][-1], grid_data[4][-1]) == ("N", "S") or
                (grid_data[2][-1], grid_data[4][-1]) == ("S", "N")):
            yres = (slat + elat) / (float(grid_data[1]) - 1)
        else:
            yres = (slat - elat) / (float(grid_data[1]) - 1)

        cdef = (
                ("{}&deg; x ~{}&deg; from {} to {} and {} to {} <small>(").
                format(grid_data[6], round(yres, 3), grid_data[3],
                       grid_data[5], grid_data[2], grid_data[4]))
        if grid_data[0] == "-1":
            cdef += "reduced n{}".format(int(grid_data[1]) / 2)
        else:
            cdef += "{} x {}".format(grid_data[0], grid_data[1])

        cdef += " Longitude/Gaussian Latitude)</small>"
        return cdef
    elif gdef[0] == "lambertConformal":
        return (
                ("{}km x {}km (at {}) oriented {} <small>({}x{} Lambert "
                 "Conformal starting at {}, {})</small>")
                .format(grid_data[7], grid_data[8], grid_data[4], grid_data[5],
                        grid_data[0], grid_data[1], grid_data[2],
                        grid_data[3]))
    elif gdef[0] == "latLon" or gdef[0] == "mercator":
        cdef = "{}&deg; x ".format(grid_data[6])
        if gdef[0] == "mercator":
            cdef += "~"

        cdef += (
                ("{}&deg; from {} to {} and {} to {} <small>(")
                .format(grid_data[7], grid_data[3], grid_data[5], grid_data[2],
                        grid_data[4]))
        if grid_data[0] == "-1":
            cdef += "reduced"
        else:
            cdef += "{} x {}".format(grid_data[0], grid_data[1])

        if gdef[0] == "latLon":
            cdef += " Latitude/Longitude"
        elif gdef[0] == "mercator":
            cdef += " Mercator"

        cdef += ")</small>"
        return cdef
    elif gdef[0] == "polarStereographic":
        cdef = "{}km x {}km (at".format(grid_data[7], grid_data[8])
        if len(grid_data[4]) > 0:
            cdef += grid_data[4]
        else:
            cdef += "60" + grid_data[6]

        if grid_data[6] == "N":
            orient = "North"
        else:
            orient = "South"

        cdef += (
                (") oriented {} <small>({} x {} {} Polar Stereographic)"
                 "</small>").format(grid_data[5], grid_data[0], grid_data[1],
                                    orient))
        return cdef
    elif gdef[0] == "sphericalHarmonics":
        cdef = "Spherical Harmonics at "
        if grid_data[2] == grid_data[0] and grid_data[0] == grid_data[1]:
            cdef += "T"
        elif int(grid_data[1]) == (int(grid_data[1]) + int(grid_data[2])):
            cdef += "R"

        cdef += "{} spectral resolution".format(grid_data[1])
        return cdef
