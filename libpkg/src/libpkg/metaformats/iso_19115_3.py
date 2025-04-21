import psycopg2

from datetime import timedelta
from lxml import etree

from . import settings
from ..xmlutils import convert_html_to_text


def add_metadata_identifier(root, nsmap, dsid):
    md_ident = (
            etree.SubElement(
                    etree.SubElement(
                            root, "{" + nsmap['mdb'] + "}metadataIdentifier"),
                    "{" + nsmap['mcc'] + "}MD_Identifier"))
    etree.SubElement(
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(
                                    md_ident,
                                    "{" + nsmap['mcc'] + "}authority"),
                            "{" + nsmap['cit'] + "}CI_Citation"),
                    "{" + nsmap['cit'] + "}title"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['name'])
    etree.SubElement(
            etree.SubElement(md_ident, "{" + nsmap['mcc'] + "}code"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    "edu.ucar.rda::" + dsid)
    etree.SubElement(
            etree.SubElement(md_ident, "{" + nsmap['mcc'] + "}codeSpace"),
            "{" + nsmap['gco'] + "}CharacterString").text = "edu.ucar.rda"


def add_contact(root, nsmap):
    ci_respons = (
            etree.SubElement(
                    etree.SubElement(
                            root, "{" + nsmap['mdb'] + "}contact"),
                    "{" + nsmap['cit'] + "}CI_Responsibility"))
    etree.SubElement(
            etree.SubElement(ci_respons, "{" + nsmap['cit'] + "}role"),
            "{" + nsmap['cit'] + "}CI_RoleCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#CI_RoleCode"),
            codeListValue="publisher").text = "publisher"
    ci_org = (
            etree.SubElement(
                    etree.SubElement(ci_respons,
                                     "{" + nsmap['cit'] + "}party"),

                    "{" + nsmap['cit'] + "}CI_Organisation"))
    etree.SubElement(
            etree.SubElement(ci_org, "{" + nsmap['cit'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['name'])
    ci_contact = (
            etree.SubElement(
                    etree.SubElement(ci_org,
                                     "{" + nsmap['cit'] + "}contactInfo"),
                    "{" + nsmap['cit'] + "}CI_Contact"))
    etree.SubElement(
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(
                                    ci_contact,
                                    "{" + nsmap['cit'] + "}address"),
                            "{" + nsmap['cit'] + "}CI_Address"),
                    "{" + nsmap['cit'] + "}electronicMailAddress"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['email'])
    ci_online = (
            etree.SubElement(
                    etree.SubElement(ci_contact,
                                     "{" + nsmap['cit'] + "}onlineResource"),
                    "{" + nsmap['cit'] + "}CI_OnlineResource"))
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}linkage"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['url'])
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}protocol"),
            "{" + nsmap['gco'] + "}CharacterString").text = "http"
    etree.SubElement(
            etree.SubElement(ci_online,
                             "{" + nsmap['cit'] + "}applicationProfile"),
            "{" + nsmap['gco'] + "}CharacterString").text = "http"
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['name'] + " Home Page")
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}function"),
            "{" + nsmap['cit'] + "}CI_OnLineFunctionCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#CI_OnLineFunctionCode"),
            codeListValue="browsing").text = "browsing"


def add_metadata_date(root, nsmap, mdate):
    ci_date = (
            etree.SubElement(
                    etree.SubElement(root, "{" + nsmap['mdb'] + "}dateInfo"),
                    "{" + nsmap['cit'] + "}CI_Date"))
    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['cit'] + "}date"),
            "{" + nsmap['gco'] + "}DateTime").text = (
                    mdate.replace(" ", "T") + "+00:00")
    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['cit'] + "}dateType"),
            "{" + nsmap['cit'] + "}CI_DateTypeCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.html#CI_DateTypeCode"),
            codeListValue="lastUpdated").text = "lastUpdated"


def add_metadata_standard(root, nsmap):
    ci_citation = (
            etree.SubElement(
                    etree.SubElement(root,
                                     "{" + nsmap['mdb'] + "}metadataStandard"),
                    "{" + nsmap['cit'] + "}CI_Citation"))
    etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['cit'] + "}title"),
            "{" + nsmap['gco'] + "}CharacterString").text = "ISO 19115-1"
    etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['cit'] + "}edition"),
            "{" + nsmap['gco'] + "}CharacterString").text = "2014"


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
        mcursor.execute(("select timestamp_utc, title, summary, pub_date, "
                         "continuing_update from search.datasets where dsid = "
                         "%s"), (dsid, ))
        tstamp, title, abstract, pub_date, progress = mcursor.fetchone()
        abstract = convert_html_to_text(
                "<abstract>" + abstract + "</abstract>")
        progress = "onGoing" if progress == "Y" else "completed"
        mcursor.execute((
                "select max(date_modified + time_modified) from dssdb.wfile_" +
                dsid))
        fdate = mcursor.fetchone()
        if fdate is not None:
            fdate = fdate[0] + timedelta(hours=6)
            tstamp = max(fdate, tstamp)

        nsmap = {
            'mdb': "http://standards.iso.org/iso/19115/-3/mdb/1.0",
            'cit': "http://standards.iso.org/iso/19115/-3/cit/1.0",
            'mri': "http://standards.iso.org/iso/19115/-3/mri/1.0",
            'mcc': "http://standards.iso.org/iso/19115/-3/mcc/1.0",
            'mco': "http://standards.iso.org/iso/19115/-3/mco/1.0",
            'mrd': "http://standards.iso.org/iso/19115/-3/mrd/1.0",
            'mmi': "http://standards.iso.org/iso/19115/-3/mmi/1.0",
            'gex': "http://standards.iso.org/iso/19115/-3/gex/1.0",
            'gco': "http://standards.iso.org/iso/19115/-3/gco/1.0",
            'gml': "http://www.opengis.net/gml/3.2",
            'xsi': "http://www.w3.org/2001/XMLSchema-instance",
        }
        schema_loc = etree.QName(
                nsmap['xsi'],
                "schemaLocation")
        xsd_head = "http://standards.iso.org/iso/19115/-3/mds/1.0"
        xsd = xsd_head + "/mds.xsd"
        root = etree.Element(
                "{" + nsmap['mdb'] + "}MD_Metadata",
                {schema_loc: " ".join([xsd_head, xsd])},
                nsmap=nsmap)
        add_metadata_identifier(root, nsmap, dsid)
        add_contact(root, nsmap)
        add_metadata_date(root, nsmap, str(tstamp))
        add_metadata_standard(root, nsmap)
    finally:
        mconn.close()
        wconn.close()

    return etree.tostring(root, pretty_print=True).decode("utf-8")
