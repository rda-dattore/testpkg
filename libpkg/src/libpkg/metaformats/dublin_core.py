import psycopg2

from lxml import etree

from . import settings
from ..metautils import open_dataset_overview
from ..xmlutils import convert_html_to_text


def export_html_meta(dsid, metadb_settings):
    pass


def export_oai_dc(dsid, metadb_settings, wagtail_settings):
    try:
        mconn = psycopg2.connect(**metadb_settings)
        mcursor = mconn.cursor()
        wconn = psycopg2.connect(**wagtail_settings)
        wcursor = wconn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    warnings = []
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
                settings.ARCHIVE['pub_name'])
        etree.SubElement(root, dc_ns + "date").text = (
                "Published: " + str(pub_date))
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
            identifier.text = "edu.ucar.rda:" + dsid

        etree.SubElement(root, dc_ns + "language").text = "english"
        dslist = xml_root.findall("./relatedDataset")
        if len(dslist) > 0:
            for ds in dslist:
                etree.SubElement(root, dc_ns + "relation").text = (
                        "rda.ucar.edu:" + ds.get("ID"))

        rsrclst = xml_root.findall("./relatedResource")
        if len(rsrclst) > 0:
            for rsrc in rsrclst:
                etree.SubElement(root, dc_ns + "relation").text = (
                        rsrc.text + " [" + rsrc.get("url") + "]")

        wcursor.execute((
                "select access_restrict from wagtail."
                "dataset_description_datasetdescriptionpage where dsid = "
                "%s"), (dsid, ))
        access = wcursor.fetchone()
        if access is not None and len(access[0]) > 0:
            etree.SubElement(root, dc_ns + "rights").text = (
                    convert_html_to_text("<access>" + access[0] +
                                         "</access>"))

        wcursor.execute((
                "select usage_restrict from wagtail."
                "dataset_description_datasetdescriptionpage where dsid = "
                "%s"), (dsid, ))
        usage = wcursor.fetchone()
        if usage is not None and len(usage[0]) > 0:
            etree.SubElement(root, dc_ns + "rights").text = (
                    convert_html_to_text("<usage>" + usage[0] + "</usage>"))

    finally:
        mconn.close()
        wconn.close()

    return (etree.tostring(root, pretty_print=True).decode("utf-8"),
            "\n".join(warnings))


def export(dsid, metadb_settings, wagtail_settings, **kwargs):
    if 'output' in kwargs and kwargs['output'] == "html_meta":
        return export_html_meta(dsid, metadb_settings)

    return export_oai_dc(dsid, metadb_settings, wagtail_settings)
