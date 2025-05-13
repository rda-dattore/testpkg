import psycopg2

from lxml import etree

from ..metautils import get_date_from_precision, open_dataset_overview


def convert_gcmd_uuids(xml_root, element, concept, cursor):
    els = xml_root.findall("./" + element + "[@vocabulary='GCMD']")
    for el in els:
        cursor.execute(
                "select path from search.gcmd_" + concept + " where uuid = %s",
                (el.text, ))
        path = cursor.fetchone()
        el.text = path[0]


def export(dsid, metadb_settings):
    try:
        conn = psycopg2.connect(**metadb_settings)
        cursor = conn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    try:
        xml_root = open_dataset_overview(dsid)
        summary = xml_root.find("./summary")
        stext = etree.tostring(summary).decode("utf-8")
        idx = stext.find(">")
        ridx = stext.rfind("<")
        stext = stext[idx+1:ridx]
        summary.text = "<![CDATA[" + stext + "]]"
        lst = summary.findall("./p")
        for p in lst:
            p.getparent().remove(p)

        authors = xml_root.findall("./author")
        for author in authors:
            type = author.get(
                    "{http://www.w3.org/2001/XMLSchema-instance}type")
            if type in (None, "authorPerson"):
                author.text = (
                        " ".join([author.get("fname"), author.get("mname"),
                                  author.get("lname")]).replace("  ", " "))
                author.attrib.pop("fname")
                author.attrib.pop("mname")
                author.attrib.pop("lname")
            else:
                author.text = author.get("name")
                author.attrib.pop("name")

        convert_gcmd_uuids(xml_root, "variable", "sciencekeywords", cursor)
        convert_gcmd_uuids(xml_root, "platform", "platforms", cursor)
        convert_gcmd_uuids(xml_root, "project", "projects", cursor)
        convert_gcmd_uuids(xml_root, "supportsProject", "projects", cursor)
        convert_gcmd_uuids(xml_root, "instrument", "instruments", cursor)
        lst = xml_root.findall("./relatedDataset")
        for el in lst:
            id = el.get("ID")
            if len(id) == 5 and id[3] == '.':
                id = 'd' + id[0:3] + "00" + id[-1]
                el.set("ID", id)

        cmd = xml_root.find("./contentMetadata")
        if cmd is None:
            el_set = {'periods': [], 'data_types': set(),
                      'data_formats': set()}
            cursor.execute((
                    "select distinct schemaname from pg_tables where "
                    "tablename like %s"), ("%" + dsid + "%", ))
            res = cursor.fetchall()
            for dtype, in res:
                if dtype == "WGrML":
                    el_set['data_types'].add("grid")

                if dtype == "WObML":
                    el_set['data_types'].add("platform_observation")

                if dtype == "WFixML":
                    el_set['data_types'].add("cyclone_fix")

                if dtype[0] == 'W':
                    cursor.execute((
                        "select distinct f.format from \"" + dtype + "\"." +
                        dsid + "_webfiles2 as w left join \"" + dtype + "\"."
                        "formats as f on f.code = w.format_code"))
                    fmts = cursor.fetchall()
                    for fmt in fmts:
                        el_set['data_formats'].add(fmt[0])

            cursor.execute((
                    "select min(concat(p.date_start, ' ', p.time_start)), "
                    "string_agg(distinct cast(p.start_flag as text), ','), "
                    "max(concat(p.date_end, ' ', p.time_end)), string_agg("
                    "distinct p.time_zone, ','), g2.grpid from dssdb.dsperiod "
                    "as p left join dssdb.dsgroup as g on g.dsid = p.dsid and "
                    "g.gindex = p.gindex left join dssdb.dsgroup as g2 on g2."
                    "dsid = p.dsid and g2.gindex = g.pindex where p.dsid = %s "
                    "group by g2.grpid"), (dsid, ))
            res = cursor.fetchall()
            if len(res) > 0:
                el_set['periods'] = [row for row in res]

            if any(el_set.values()):
                cmd = etree.SubElement(xml_root, "contentMetadata")
                for dtype in el_set['data_types']:
                    etree.SubElement(cmd, "dataType").text = dtype

                for period in el_set['periods']:
                    temporal = etree.SubElement(
                            cmd, "temporal",
                            start=get_date_from_precision(
                                    period[0], int(period[1]), period[3]),
                            end=get_date_from_precision(
                                    period[2], int(period[1]), period[3]))
                    if period[4] is None:
                        temporal.set("groupID", "Entire Dataset")
                    else:
                        temporal.set("groupID", period[4])

                for fmt in el_set['data_formats']:
                    etree.SubElement(cmd, "format").text = fmt

    finally:
        conn.close()

    return etree.tostring(xml_root, pretty_print=True).decode("utf-8")
