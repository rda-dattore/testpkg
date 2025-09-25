import psycopg2

from lxml import etree

from . import settings
from ..metautils import get_date_from_precision, open_dataset_overview
from ..xmlutils import convert_html_to_text


def export_html_meta(dsid, metadb_settings):
    try:
        conn = psycopg2.connect(**metadb_settings)
        cursor = conn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    try:
        meta_tags = []
        meta_tags.append('<meta name="DC.type" content="Dataset"/>')
        cursor.execute((
                "select doi from dssdb.dsvrsn where dsid = %s and status = "
                "'A'"), (dsid, ))
        doi = cursor.fetchone()
        if doi is not None and len(doi[0]) > 0:
            identifier = "https://doi.org/" + doi[0]
        else:
            identifier = ("https://" + settings.ARCHIVE['domain'] + "/" +
                          settings.ARCHIVE['datasets_path'] + "/" + dsid)

        meta_tags.append(
                '<meta name="DC.identifier" content="' + identifier + '"/>')
        cursor.execute((
                "select type, given_name, middle_name, family_name from "
                "search.dataset_authors where dsid = %s"), (dsid, ))
        authors = cursor.fetchall()
        if len(authors) > 0:
            for author in authors:
                if author[0] == "Person":
                    meta_tags.append((
                            '<meta name="DC.creator" content="' + author[3] +
                            ', ' + (author[1] + ' ' + author[2]).strip() +
                            '"/>'))
                else:
                    meta_tags.append((
                            '<meta name="DC.creator" content="' + author[1] +
                            '"/>'))
        else:
            cursor.execute((
                    "select g.path,c.contact from search.contributors_new as "
                    "c left join search.gcmd_providers as g on g.uuid = c."
                    "keyword where c.dsid = %s and c.vocabulary = 'GCMD'"),
                    (dsid, ))
            contributors = cursor.fetchall()
            if len(contributors) == 0:
                raise RuntimeError("no contributors were found for " + dsid)

            for contributor in contributors:
                parts = contributor[0].split(" > ")
                if parts[-1] == "UNAFFILIATED INDIVIDUAL":
                    idx = contributor[1].find(",")
                    if idx < 0:
                        idx = None

                    meta_tags.append((
                            '<meta name="DC.creator" content="' +
                            contributor[1][0:idx] + '"/>'))
                else:
                    meta_tags.append((
                            '<meta name="DC.creator" content="' +
                            parts[-1].replace(", ", "/") + '"/>'))

        cursor.execute((
                "select title, pub_date, summary from search.datasets where "
                "dsid = %s"), (dsid, ))
        res = cursor.fetchone()
        meta_tags.append('<meta name="DC.title" content="' + res[0] + '"/>')
        meta_tags.append((
                '<meta name="DC.date" content="' + str(res[1]) +
                '" scheme="DCTERMS.W3CDTF"/>'))
        meta_tags.append((
                '<meta name="DC.publisher" content="' +
                settings.ARCHIVE['pub_name']['default'] + '"/>'))
        summary = convert_html_to_text("<summary>" + res[2] + "</summary>")
        meta_tags.append((
                '<meta name="DC.description" content="' +
                summary.replace("\n", "\\n") + '"/>'))
        cursor.execute((
                "select g.path from search.variables as v left join search."
                "gcmd_sciencekeywords as g on g.uuid = v.keyword where v."
                "dsid = %s and v.vocabulary = 'GCMD'"), (dsid, ))
        subjects = cursor.fetchall()
        for subject in subjects:
            meta_tags.append(
                    '<meta name="DC.subject" content="' + subject[0] + '"/>')
    finally:
        conn.close()

    return "\n".join(meta_tags)


def export_oai_dc(dsid, metadb_settings, wagtail_settings):
    try:
        mconn = psycopg2.connect(**metadb_settings)
        mcursor = mconn.cursor()
        wconn = psycopg2.connect(**wagtail_settings)
        wcursor = wconn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    try:
        mcursor.execute((
                "select title, summary, pub_date from search.datasets where "
                "dsid = %s"), (dsid, ))
        title, summary, pub_date = mcursor.fetchone()
        summary = convert_html_to_text("<summary>" + summary + "</summary>")
        nsmap = {
            'oai_dc': "http://www.openarchives.org/OAI/2.0/oai_dc/",
            'dc': "http://purl.org/dc/elements/1.1/",
            'xsi': "http://www.w3.org/2001/XMLSchema-instance",
        }
        schema_loc = etree.QName(
                nsmap['xsi'],
                "schemaLocation")
        xsd = "http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
        root = etree.Element(
                "{" + nsmap['oai_dc'] + "}dc",
                {schema_loc: " ".join([nsmap['oai_dc'], xsd])},
                nsmap=nsmap)
        xml_root = open_dataset_overview(dsid)
        dc_ns = "{" + nsmap['dc'] + "}"
        etree.SubElement(root, dc_ns + "title").text = title
        mcursor.execute((
                "select type, given_name, middle_name, family_name from "
                "search.dataset_authors where dsid = %s"), (dsid, ))
        authors = mcursor.fetchall()
        if len(authors) > 0:
            for author in authors:
                creator = etree.SubElement(root, dc_ns + "creator")
                if author[0] == "Person":
                    creator.text = author[1]
                    if len(author[2]) > 0:
                        creator.text += " " + author[2]

                    creator.text += " " + author[3]
                else:
                    creator.text = author[1]

            cname = "contributor"
        else:
            cname = "creator"

        mcursor.execute((
                "select g.path from search.contributors_new as c left join "
                "search.gcmd_providers as g on g.uuid = c.keyword where c."
                "dsid = %s and c.vocabulary = 'GCMD'"), (dsid, ))
        contributors = mcursor.fetchall()
        for contributor in contributors:
            etree.SubElement(root, dc_ns + cname).text = (
                    contributor[0])

        etree.SubElement(root, dc_ns + "publisher").text = (
                settings.ARCHIVE['pub_name']['default'])
        etree.SubElement(root, dc_ns + "date").text = (
                "Published: " + str(pub_date))
        mcursor.execute((
                "select min(concat(date_start, ' ', time_start)), min("
                "start_flag), max(concat(date_end, ' ', time_end)), min("
                "end_flag), min(time_zone) from dssdb.dsperiod where dsid = "
                "%s and date_start < '9998-01-01' and date_end < "
                "'9998-01-01'"), (dsid, ))
        res = mcursor.fetchone()
        if res is not None and all(res):
            tz = res[4]
            idx = tz.find(",")
            if idx > 0:
                tz = tz[0:idx]

            etree.SubElement(root, dc_ns + "date").text = (
                "Valid: " + get_date_from_precision(res[0], res[1], tz) +
                " to " + get_date_from_precision(res[2], res[3], tz))

        mcursor.execute((
                "select keyword from search.topics where dsid = %s and "
                "vocabulary = 'ISO'"), (dsid, ))
        topic, = mcursor.fetchone()
        etree.SubElement(root, dc_ns + "subject").text = topic
        etree.SubElement(root, dc_ns + "description").text = summary
        etree.SubElement(root, dc_ns + "type").text = "Dataset"
        identifier = etree.SubElement(root, dc_ns + "identifier")
        mcursor.execute((
                "select doi from dssdb.dsvrsn where dsid = %s and status = "
                "'A'"), (dsid, ))
        doi = mcursor.fetchone()
        if doi is not None:
            identifier.text = "DOI:" + doi[0]
        else:
            parts = settings.ARCHIVE['domain'].split(".")
            identifier.text = ".".join(reversed(parts)) + ":" + dsid

        etree.SubElement(root, dc_ns + "language").text = "english"
        dslist = xml_root.findall("./relatedDataset")
        if len(dslist) > 0:
            dparts = settings.ARCHIVE['domain'].split(".")
            dparts.reverse()
            for ds in dslist:
                etree.SubElement(root, dc_ns + "relation").text = (
                        "oai:" + ".".join(dparts) + ":" + ds.get("ID"))

        rsrclst = xml_root.findall("./relatedResource")
        if len(rsrclst) > 0:
            for rsrc in rsrclst:
                etree.SubElement(root, dc_ns + "relation").text = (
                        rsrc.text + " [" + rsrc.get("url") + "]")

        wcursor.execute((
                "select access_restrict from wagtail2."
                "dataset_description_datasetdescriptionpage where dsid = "
                "%s"), (dsid, ))
        access = wcursor.fetchone()
        if access is not None and len(access[0]) > 0:
            etree.SubElement(root, dc_ns + "rights").text = (
                    convert_html_to_text("<access>" + access[0] +
                                         "</access>"))

        wcursor.execute((
                "select usage_restrict from wagtail2."
                "dataset_description_datasetdescriptionpage where dsid = "
                "%s"), (dsid, ))
        usage = wcursor.fetchone()
        if usage is not None and len(usage[0]) > 0:
            etree.SubElement(root, dc_ns + "rights").text = (
                    convert_html_to_text("<usage>" + usage[0] + "</usage>"))

    finally:
        mconn.close()
        wconn.close()

    return etree.tostring(root, pretty_print=True).decode("utf-8")


def export(dsid, metadb_settings, wagtail_settings, **kwargs):
    if 'output' in kwargs and kwargs['output'] == "html_meta":
        return export_html_meta(dsid, metadb_settings)

    return export_oai_dc(dsid, metadb_settings, wagtail_settings)
