import psycopg2

from lxml import etree

from ..metautils import open_dataset_overview


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
            if type == "authorPerson":
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
            periods = []
            cursor.execute((
                    "select min(concat(p.date_start, ' ', p.time_start)), "
                    "string_agg(distinct cast(p.start_flag as text), ','), "
                    "max(concat(p.date_end, ' ', p.time_end)), string_agg("
                    "distinct p.time_zone, ','), g.pindex from dssdb.dsperiod "
                    "as p left join dssdb.dsgroup as g on g.gindex = p.gindex "
                    "where p.dsid = %s group by g.pindex"), (dsid, ))
            res = cursor.fetchall()
            if len(res) > 0:
                periods = [row for row in res]

    finally:
        conn.close()

    return etree.tostring(xml_root, pretty_print=True).decode("utf-8")
