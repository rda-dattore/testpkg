import re


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


def spatial_domain_from_grid_definition(gdef, **kwargs):
    domain = {'wlon': None, 'slat': None, 'elon': None, 'nlat': None}
    parts = gdef[1].split(":")
    if re.compile("^(latLon|gaussLatLon|mercator)(Cell){0,1}$").match(gdef[0]):
        start_lat = decode_latitude(parts[2])
        end_lat = decode_latitude(parts[4])
        domain['slat'] = min(start_lat, end_lat)
        domain['nlat'] = max(start_lat, end_lat)
        start_elon = decode_elongitude(parts[3])
        end_elon = decode_elongitude(parts[5])
        xspace = float(parts[0])
        xres = float(parts[6])
        if xspace == 0.:
            # reduced grids
            xspace = (end_elon - start_elon) / xres
        else:
            xspace -= 1.

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
                float(parts[7])) - 180.) < 0.001):
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

        elif 'centerOn' in kwargs and kwargs['centerOn'] == "primeMeridianl":
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
        pass
    elif gdef[0].find("polarStereographic") == 0:
        pass

    return domain
