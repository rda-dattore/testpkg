import json
import os
import psycopg2

from . import settings
from ..geospatial import fill_geographic_extent_data
from ..metautils import open_dataset_overview
from ..xmlutils import convert_html_to_text


def export(dsid, metadb_settings, **kwargs):
    try:
        conn = psycopg2.connect(**metadb_settings)
        cursor = conn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    try:
        cursor.execute(("select s.title, s.summary, s.pub_date, v.doi from "
                        "search.datasets as s left join dssdb.dsvrsn as v on "
                        "v.dsid = s.dsid where s.dsid = %s and v.end_date is "
                        "null"), (dsid, ))
        res = cursor.fetchone()
        if res is None:
            raise RuntimeError("dataset doesn't exist or could not be found")

        summary = convert_html_to_text(
                "<summary>" + res[1].replace("&amp;", "&") + "</summary>")
        if res[3] is not None and len(res[3]) > 0:
            id = os.path.join(settings.DOI_DOMAIN, res[3])
        else:
            id = os.path.join("https://", settings.ARCHIVE['domain'],
                              settings.ARCHIVE['datasets_path'], dsid)

        jsonld_data = {
            '@context': {
                '@language': "en",
                '@vocab': "https://schema.org/",
                'sc': "https://schema.org/",
                'cr': "http://mlcommons.org/croissant/",
                'rai': "http://mlcommons.org/croissant/RAI/",
                'dct': "http://purl.org/dc/terms/",
            },
            '@type': "Dataset",
            '@id': id,
            'name': res[0],
            'description': summary,
            'publisher': {
                '@type': "Organization",
                'name': settings.ARCHIVE['pub_name']['default'],
            },
            'datePublished': str(res[2]),
            'author': {},
        }
        xml_root = open_dataset_overview(dsid)
        alst = []
        lst = xml_root.findall("./author")
        if len(lst) > 0:
            for e in lst:
                type = e.get("{http://www.w3.org/2001/XMLSchema-instance}type")
                if type is None or type == "authorPerson":
                    d = {
                            '@type': "Person",
                            'givenName': e.get("fname"),
                            'familyName': e.get("lname"),
                        }
                else:
                    d = {
                            '@type': "Organization",
                            'name': e.get("name"),
                        }

                alst.append(d)

        else:
            cursor.execute(("select g.path, c.contact from search."
                            "contributors_new as c left join search."
                            "gcmd_providers as g on g.uuid = c.keyword where "
                            "c.dsid = %s and c.vocabulary = 'GCMD'"), (dsid, ))
            res = cursor.fetchall()
            for e in res:
                name_parts = e[0].split(" > ")
                if name_parts[-1] == "UNAFFILIATED INDIVIDUAL":
                    if len(e[1]) > 0:
                        contact_parts = e[1].split(",")
                        d = {
                                '@type': "Person",
                                'name': contact_parts[0],
                        }

                else:
                    d = {
                            '@type': "Organization",
                            'name': name_parts[-1].replace(", ", "/")
                    }

                alst.append(d)

        if len(alst) == 0:
            raise RuntimeError(("no authors or contributors could be "
                                "identified"))

        if len(alst) > 1:
            jsonld_data['author'].update({'@list': []})
            for a in alst:
                jsonld_data['author']['@list'].append(a)

        else:
            jsonld_data['author'].update(alst[0])

        cursor.execute(("select g.path from search.variables as v left join "
                        "search.gcmd_sciencekeywords as g on g.uuid = v."
                        "keyword where v.dsid = %s and v.vocabulary = 'GCMD'"),
                       (dsid, ))
        res = cursor.fetchall()
        if len(res) > 0:
            if len(res) > 1:
                jsonld_data['keywords'] = [e[0] for e in res]
            else:
                jsonld_data['keywords'] = res[0][0]

        cursor.execute(("select min(date_start), min(time_start), max("
                        "date_end), max(time_end), min(start_flag), min("
                        "time_zone) from dssdb.dsperiod where dsid = %s and "
                        "date_start < '9998-01-01' and date_end < "
                        "'9998-01-01' group by dsid"), (dsid, ))
        res = cursor.fetchone()
        if res is not None:
            num_parts = int(res[4])
            sdate = str(res[0])
            edate = str(res[2])
            if num_parts < 3:
                chop = 3 * (3 - num_parts)
                sdate = sdate[:-chop]
                edate = edate[:-chop]
            else:
                sdate += "T" + str(res[1])
                edate += "T" + str(res[3])
                if num_parts < 6:
                    chop = 3 * (6 - num_parts)
                    sdate = sdate[:-chop]
                    edate = edate[:-chop]

            jsonld_data['temporalCoverage'] = sdate + "/" + edate

        geoext = fill_geographic_extent_data(dsid, cursor)
        if all(geoext.values()):
            jsonld_data['spatialCoverage'] = {'@type': "Place"}
            if (geoext['wlon'] == geoext['elon'] and geoext['slat'] ==
                    geoext['nlat']):
                d = {
                    '@type': "GeoCoordinates",
                    'latitude': geoext['slat'],
                    'longitude': geoext['wlon'],
                }
            else:
                d = {
                    '@type': "GeoShape",
                    'box': " ".join([
                            ",".join([str(geoext['slat']),
                                      str(geoext['wlon'])]),
                            ",".join([str(geoext['nlat']),
                                      str(geoext['elon'])])]),
                }

            jsonld_data['spatialCoverage']['geo'] = d

        license = xml_root.find("./dataLicense")
        if license is not None:
            jsonld_data['license'] = license.text
        else:
            raise RuntimeError("no data license could be identified")

    finally:
        conn.close()

    indent = kwargs['indent'] if 'indent' in kwargs else None
    return json.dumps(jsonld_data, indent=indent)
