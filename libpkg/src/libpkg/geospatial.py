from .gridutils import spatial_domain_from_grid_definition


def fill_geographic_extent_data(dsid, cursor):
    geo_data = {'wlon': None, 'slat': None, 'elon': None, 'nlat': None}
    cursor.execute(("select distinct d.definition, d.def_params from "
                    "\"WGrML\".summary as s left join \"WGrML\"."
                    "grid_definitions as d on d.code = s.grid_definition_code "
                    "where s.dsid = %s"), (dsid, ))
    res = cursor.fetchall()
    if len(res) > 0:
        for e in res:
            domain = spatial_domain_from_grid_definition(
                    e, centerOn="primeMeridian")
            if all(domain.values()):
                geo_data['wlon'] = (
                        domain['wlon'] if geo_data['wlon'] is None else
                        min(domain['wlon'], geo_data['wlon']))
                geo_data['slat'] = (
                        domain['slat'] if geo_data['slat'] is None else
                        min(domain['slat'], geo_data['slat']))
                geo_data['elon'] = (
                        domain['elon'] if geo_data['elon'] is None else
                        max(domain['elon'], geo_data['elon']))
                geo_data['nlat'] = (
                        domain['nlat'] if geo_data['nlat'] is None else
                        max(domain['nlat'], geo_data['nlat']))

    else:
        pass

    cursor.execute(("select tablename from pg_tables where tablename = '" +
                    dsid + "_geobounds' and schemaname = 'WObML'"))
    res = cursor.fetchone()
    if res is not None:
        cursor.execute(("select min(min_lat), min(min_lon), max(max_lat), max("
                        "max_lon) from \"WObML\"." + dsid + "_geobounds where "
                        "min_lat >= -900000 and min_lon >= -1800000 and "
                        "max_lat <= 900000 and max_lon <= 1800000"))
        res = cursor.fetchall()
    else:
        cursor.execute(("select l.keyword, d.box1d_row, d.box1d_bitmap_min, d."
                        "box1d_bitmap_max from search.locations as l left "
                        "join search.location_data as d on d.keyword = l."
                        "keyword and d.vocabulary = l.vocabulary where l.dsid "
                        "= %s and d.box1d_row >= 0"), (dsid, ))
        res = cursor.fetchall()
        if res is not None:
            pass
        else:
            pass

    return geo_data
