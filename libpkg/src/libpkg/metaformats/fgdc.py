import os
import psycopg2

from lxml import etree

from . import settings
from ..metautils import open_dataset_overview
from ..strutils import snake_to_capital
from ..xmlutils import convert_html_to_text


def export(dsid, metadb_settings, wagtaildb_settings):
    try:
        mconn = psycopg2.connect(**metadb_settings)
        mcursor = mconn.cursor()
        wconn = psycopg2.connect(**wagtaildb_settings)
        wcursor = wconn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    try:
        nsmap = {
            'xsi': "http://www.w3.org/2001/XMLSchema-instance",
        }
        schema_loc = etree.QName(
                nsmap['xsi'],
                "schemaLocation")
        xsd_loc = "http://www.fgdc.gov/schemas/metadata"
        xsd = os.path.join(xsd_loc, "fgdc-std-001-1998.xsd")
        root = etree.Element("metadata",
                             {schema_loc: " ".join([xsd_loc, xsd])},
                             nsmap=nsmap)
        xml_root = open_dataset_overview(dsid)
        idinfo = etree.SubElement(root, "idinfo")
        citeinfo = etree.SubElement(
                etree.SubElement(idinfo, "citation"), "citeinfo")
        mcursor.execute((
                "select type, given_name, middle_name, family_name from "
                "search.dataset_authors where dsid = %s"), (dsid, ))
        alist = mcursor.fetchall()
        authors = []
        if len(alist) > 0:
            for atype, fname, mname, lname in alist:
                if atype == "Person":
                    author = fname
                    if len(mname) > 0:
                        author += " " + mname

                    author += " " + lname
                else:
                    author = fname

                authors.append(author)
        else:
            mcursor.execute((
                    "select g.path from search.contributors_new as c left "
                    "join search.gcmd_providers as g on g.uuid = c.keyword "
                    "where c.dsid = %s and c.vocabulary = 'GCMD'"), (dsid, ))
            clist = mcursor.fetchall()
            for contributor in clist:
                authors.append(contributor[0])

        etree.SubElement(citeinfo, "origin").text = ", ".join(authors)
        mcursor.execute((
                "select title, summary, pub_date, continuing_update from "
                "search.datasets where dsid = %s"), (dsid, ))
        title, summary, pub_date, progress = mcursor.fetchone()
        summary = convert_html_to_text("<summary>" + summary + "</summary>")
        etree.SubElement(citeinfo, "pubdate").text = (
                str(pub_date).replace("-", ""))
        etree.SubElement(citeinfo, "title").text = title
        pubinfo = etree.SubElement(citeinfo, "pubinfo")
        etree.SubElement(pubinfo, "pubplace").text = "Boulder, CO"
        etree.SubElement(pubinfo, "publish").text = (
                "Research Data Archive at the NSF National Center for "
                "Atmospheric Research, Computational and Information Systems "
                "Laboratory")
        onlink = etree.SubElement(citeinfo, "onlink")
        mcursor.execute((
                "select doi from dssdb.dsvrsn where dsid = %s and status = "
                "'A'"), (dsid, ))
        doi = mcursor.fetchone()
        if doi is not None:
            onlink.text = os.path.join(settings.DOI_DOMAIN, doi[0])
        else:
            onlink.text = os.path.join(settings.ARCHIVE['datasets_url'], dsid)

        descript = etree.SubElement(idinfo, "descript")
        etree.SubElement(descript, "abstract").text = summary
        etree.SubElement(descript, "purpose").text = "Not captured"
        timeperd = etree.SubElement(idinfo, "timeperd")
        mcursor.execute((
                "select min(date_start), max(date_end) from dssdb.dsperiod "
                "where dsid = %s and date_start < '9998-01-01' and date_end < "
                "'9998-01-01'"), (dsid, ))
        dates = mcursor.fetchone()
        if dates is not None:
            rngdates = etree.SubElement(
                    etree.SubElement(timeperd, "timeinfo"), "rngdates")
            etree.SubElement(rngdates, "begdate").text = (
                    str(dates[0]).replace("-", ""))
            etree.SubElement(rngdates, "enddate").text = (
                    str(dates[1]).replace("-", ""))
        else:
            sngdate = etree.SubElement(
                    etree.SubElement(timeperd, "timeinfo"), "sngdate")
            etree.SubElement(sngdate, "caldate").text = "9999"

        etree.SubElement(timeperd, "current").text = "ground condition"
        status = etree.SubElement(idinfo, "status")
        if progress == "Y":
            etree.SubElement(status, "progress").text = "In work"
            update = xml_root.find("./continuingUpdate")
            etree.SubElement(status, "update").text = (
                    snake_to_capital(update.get('frequency')))
        else:
            etree.SubElement(status, "progress").text = "Complete"
            etree.SubElement(status, "update").text = "None planned"

        keywords = etree.SubElement(idinfo, "keywords")
        mcursor.execute((
                "select g.path from search.variables as v left join search."
                "gcmd_sciencekeywords as g on g.uuid = v.keyword where v."
                "dsid = %s and v.vocabulary = 'GCMD'"), (dsid, ))
        klist = mcursor.fetchall()
        for keyword in klist:
            etree.SubElement(
                    etree.SubElement(keywords, "theme"), "themekt").text = (
                    "GCMD")
            etree.SubElement(
                    etree.SubElement(keywords, "theme"), "themekey").text = (
                    keyword[0])

        accconst = etree.SubElement(idinfo, "accconst")
        wcursor.execute((
                "select access_restrict from wagtail."
                "dataset_description_datasetdescriptionpage where dsid = %s"),
                (dsid, ))
        acc = wcursor.fetchone()
        if acc is not None and len(acc[0]) > 0:
            accconst.text = convert_html_to_text(("<access>" + acc[0] +
                                                  "</access>"))
        else:
            accconst.text = "None"

        useconst = etree.SubElement(idinfo, "useconst")
        wcursor.execute((
                "select usage_restrict from wagtail."
                "dataset_description_datasetdescriptionpage where dsid = %s"),
                (dsid, ))
        use = wcursor.fetchone()
        if use is not None and len(use[0]) > 0:
            useconst.text = convert_html_to_text(("<usage>" + use[0] +
                                                  "</usage>"))
        else:
            useconst.text = "None"

        cntinfo = etree.SubElement(
                etree.SubElement(idinfo, "ptcontac"), "cntinfo")
        etree.SubElement(
                etree.SubElement(cntinfo, "cntorgp"), "cntorg").text = (
                settings.ARCHIVE['name'])
        etree.SubElement(cntinfo, "cntemail").text = settings.ARCHIVE['email']
        metainfo = etree.SubElement(root, "metainfo")
        cntinfo = etree.SubElement(
                etree.SubElement(metainfo, "metc"), "cntinfo")
        etree.SubElement(
                etree.SubElement(cntinfo, "cntorgp"), "cntorg").text = (
                settings.ARCHIVE['name'])
        etree.SubElement(cntinfo, "cntemail").text = settings.ARCHIVE['email']
        etree.SubElement(metainfo, "metstdn").text = (
                "FGDC Content Standard for Digital Geospatial Metadata")
        etree.SubElement(metainfo, "metstdv").text = "FGDC-STD-001-1998"
    finally:
        mconn.close()
        wconn.close()

    return etree.tostring(root, pretty_print=True).decode("utf-8")
