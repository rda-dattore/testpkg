import os
import psycopg2

from datetime import timedelta
from lxml import etree

from . import settings
from ..geospatial import fill_geographic_extent_data
from ..metautils import get_date_from_precision
from ..strutils import snake_to_capital
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
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['url'][
                            0:settings.ARCHIVE['url'].find(":")])
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


def add_metadata_date(root, nsmap, cursor, dsid):
    ci_date = (
            etree.SubElement(
                    etree.SubElement(root, "{" + nsmap['mdb'] + "}dateInfo"),
                    "{" + nsmap['cit'] + "}CI_Date"))
    cursor.execute(("select timestamp_utc from search.datasets where dsid = "
                    "%s"), (dsid, ))
    tstamp, = cursor.fetchone()
    cursor.execute((
           "select max(date_modified + time_modified) from dssdb.wfile_" +
           dsid))
    fdate = cursor.fetchone()
    if fdate is not None:
        fdate = fdate[0] + timedelta(hours=6)
        tstamp = str(max(fdate, tstamp))

    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['cit'] + "}date"),
            "{" + nsmap['gco'] + "}DateTime").text = (
                    tstamp.replace(" ", "T") + "+00:00")
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


def add_alt_metadata_ref(root, nsmap):
    ci_citation = (
            etree.SubElement(
                    etree.SubElement(
                            root, ("{" + nsmap['mdb'] +
                                   "}alternativeMetadataReference")),
                    "{" + nsmap['cit'] + "}CI_Citation"))
    etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['cit'] + "}title"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['name'] + " OAI-PMH Metadata Server")
    ci_online = (
            etree.SubElement(
                    etree.SubElement(ci_citation,
                                     "{" + nsmap['cit'] + "}onlineResource"),
                    "{" + nsmap['cit'] + "}CI_OnlineResource"))
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}linkage"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    os.path.join(settings.ARCHIVE['url'],
                                 "cgi-bin/oai?verb=Identify"))
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}protocol"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['url'][
                            0:settings.ARCHIVE['url'].find(":")])
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['name'] + " OAI-PMH Metadata Server")
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}function"),
            "{" + nsmap['cit'] + "}CI_OnLineFunctionCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#CI_OnLineFunctionCode"),
            codeListValue="completeMetadata").text = "completeMetadata"


def add_metadata_linkage(root, nsmap, dsid):
    ci_online = (
            etree.SubElement(
                    etree.SubElement(root,
                                     "{" + nsmap['mdb'] + "}metadataLinkage"),
                    "{" + nsmap['cit'] + "}CI_OnlineResource"))
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}linkage"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    os.path.join(settings.ARCHIVE['url'],
                                 ("cgi-bin/oai?verb=GetRecord&identifier="
                                  "oai:edu.ucar.rda:" + dsid +
                                  "&metadataPrefix=iso19115-3")))
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}protocol"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['url'][
                            0:settings.ARCHIVE['url'].find(":")])
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    "ISO 19115-3 Metadata Record for " + dsid)
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}function"),
            "{" + nsmap['cit'] + "}CI_OnLineFunctionCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#CI_OnLineFunctionCode"),
            codeListValue="completeMetadata").text = "completeMetadata"


def add_authors(root, nsmap, cursor, dsid):
    cursor.execute((
            "select type, given_name, middle_name, family_name from search."
            "dataset_authors where dsid = %s"), (dsid, ))
    authors = cursor.fetchall()
    if len(authors) == 0:
        cursor.execute((
                "select 'Organization', g.last_in_path from search."
                "contributors_new as c left join search.gcmd_providers as g "
                "on g.uuid = c.keyword where c.dsid = %s and c.vocabulary = "
                "'GCMD'"), (dsid, ))
        authors = cursor.fetchall()

    ci_respons = (
            etree.SubElement(
                    etree.SubElement(root,
                                     ("{" + nsmap['cit'] +
                                      "}citedResponsibleParty")),
                    "{" + nsmap['cit'] + "}CI_Responsibility"))
    for author in authors:
        party = etree.SubElement(ci_respons, "{" + nsmap['cit'] + "}party")
        etree.SubElement(
                etree.SubElement(ci_respons, "{" + nsmap['cit'] + "}role"),
                "{" + nsmap['cit'] + "}CI_RoleCode",
                codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                          "codelists.xml#CI_RoleCode"),
                codeListValue="author").text = "author"
        if author[0] == "Person":
            name = author[1]
            if len(author[2]) > 0:
                name += " " + author[2]

            name += " " + author[3]
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(party,
                                             ("{" + nsmap['cit'] +
                                              "}CI_Individual")),
                            "{" + nsmap['cit'] + "}name"),
                    "{" + nsmap['gco'] + "}CharacterString").text = name
        else:
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(party,
                                             ("{" + nsmap['cit'] +
                                              "}CI_Organisation")),
                            "{" + nsmap['cit'] + "}name"),
                    "{" + nsmap['gco'] + "}CharacterString").text = author[1]


def add_extent(root, nsmap, cursor, dsid, geoext):
    extents = {'point': False, 'box': False, 'location': False,
               'temporal': False}
    if all(geoext.values()):
        if 'wlon' in geoext:
            if (geoext['wlon'] == geoext['elon'] and geoext['slat'] ==
                    geoext['nlat']):
                extents['point'] = True
            else:
                extents['box'] = True

    cursor.execute(("select min(concat(date_start, ' ', time_start)), min("
                    "start_flag), max(concat(date_end, ' ', time_end)), min("
                    "end_flag), min(time_zone) from dssdb.dsperiod where dsid "
                    "= %s and date_start < '9998-01-01' and date_end < "
                    "'9998-01-01'"), (dsid, ))
    res = cursor.fetchone()
    if res is not None:
        extents['temporal'] = True
        tz = res[4]
        idx = tz.find(",")
        if idx > 0:
            tz = tz[0:idx]

        beg_date = get_date_from_precision(res[0], res[1], tz)
        end_date = get_date_from_precision(res[2], res[3], tz)

    if not any(extents.values()):
        return

    ex_extent = (
            etree.SubElement(
                    etree.SubElement(root, "{" + nsmap['mri'] + "}extent"),
                    "{" + nsmap['gex'] + "}EX_Extent"))
    if extents['box']:
        bbox = (
                etree.SubElement(
                        etree.SubElement(ex_extent,
                                         ("{" + nsmap['gex'] +
                                          "}geographicElement")),
                        "{" + nsmap['gex'] + "}EX_GeographicBoundingBox"))
        etree.SubElement(
                etree.SubElement(bbox,
                                 "{" + nsmap['gex'] + "}westBoundLongitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['wlon'])
        etree.SubElement(
                etree.SubElement(bbox,
                                 "{" + nsmap['gex'] + "}eastBoundLongitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['elon'])
        etree.SubElement(
                etree.SubElement(bbox,
                                 "{" + nsmap['gex'] + "}southBoundLatitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['slat'])
        etree.SubElement(
                etree.SubElement(bbox,
                                 "{" + nsmap['gex'] + "}northBoundLatitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['nlat'])

    if extents['temporal']:
        id = etree.QName(nsmap['gml'], "id")
        time_period = (
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        etree.SubElement(
                                                root,
                                                ("{" + nsmap['gex'] +
                                                 "}temporalElement")),
                                        ("{" + nsmap['gex'] +
                                         "}EX_TemporalExtent")),
                                "{" + nsmap['gex'] + "}extent"),
                        "{" + nsmap['gml'] + "}TimePeriod",
                        {id: dsid + "_time_period"}, nsmap=nsmap))
        etree.SubElement(time_period,
                         "{" + nsmap['gml'] + "}beginPosition").text = beg_date
        etree.SubElement(time_period,
                         "{" + nsmap['gml'] + "}endPosition").text = end_date


def add_references(root, nsmap):
    pass


def add_maint_frequency(root, nsmap, cursor, dsid):
    cursor.execute((
            "select update_freq from wagtail."
            "dataset_description_datasetdescriptionpage where dsid = %s"),
            (dsid, ))
    freq, = cursor.fetchone()
    if freq == "Bi-monthly":
        freq = "monthly"
    elif freq == "Half-yearly":
        freq = "biannually"
    elif freq == "Yearly":
        freq = "annually"
    elif freq == "Irregularly":
        freq = "irregular"

    etree.SubElement(
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(root,
                                             ("{" + nsmap['mri'] +
                                              "}resourceMaintenance")),
                            "{" + nsmap['mmi'] + "}MD_MaintenanceInformation"),
                    "{" + nsmap['mmi'] + "}maintenanceAndUpdateFrequency"),
            "{" + nsmap['mmi'] + "}MD_MaintenanceFrequencyCode",
            codeList=("http://standards.iso.org/iso/19115/resources/Codelists/"
                      "cat/codelists.xml#MD_MaintenanceFrequencyCode"),
            codeListValue=freq).text = freq


def add_graphic_overview(root, nsmap, graphic):
    md_graphic = (
            etree.SubElement(
                    etree.SubElement(
                            root, "{" + nsmap['mri'] + "}graphicOverview"),
                    "{" + nsmap['mcc'] + "}MD_BrowseGraphic"))
    etree.SubElement(
            etree.SubElement(md_graphic, "{" + nsmap['mcc'] + "}fileName"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    os.path.join(settings.ARCHIVE['url'], "images/ds_logos",
                                 graphic))
    etree.SubElement(
            etree.SubElement(
                    md_graphic, "{" + nsmap['mcc'] + "}fileDescription"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    "Logo thumbnail from URL")
    etree.SubElement(
            etree.SubElement(md_graphic, "{" + nsmap['mcc'] + "}fileType"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    graphic[(graphic.rfind(".")+1):])


def add_data_formats(root, nsmap, cursor, dsid):
    cursor.execute((
            "select distinct keyword from search.formats where dsid = %s"),
            (dsid, ))
    dfmts = cursor.fetchall()
    for dfmt in dfmts:
        ci_citation = (
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        etree.SubElement(
                                                root, ("{" + nsmap['mri'] +
                                                       "}resourceFormat")),
                                        "{" + nsmap['mrd'] + "}MD_Format"),
                                ("{" + nsmap['mrd'] +
                                 "}formatSpecificationCitation")),
                        "{" + nsmap['cit'] + "}CI_Citation"))
        etree.SubElement(
                etree.SubElement(ci_citation, "{" + nsmap['cit'] + "}title"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                        snake_to_capital(dfmt[0]))


def add_gcmd_keywords(root, nsmap, cursor, dsid, concept):
    cursor.execute((
            "select version from search.gcmd_versions where concept_scheme = "
            "%s"), (concept, ))
    edition, = cursor.fetchone()
    concept_map = {
        'sciencekeywords': {
            'db_tables': ["variables"],
            'list_type': "Science and Services",
        },
        'platforms': {
            'db_tables': ["platforms_new"],
            'list_type': "Platforms",
        },
        'projects': {
            'db_tables': ["projects_new", "supported_projects",
                          "gcmd_projects"],
            'list_type': "Projects",
        },
        'instruments': {
            'db_tables': ["instruments"],
            'list_type': "Instruments",
        },
    }
    if len(concept_map[concept]['db_tables']) > 1:
        ulst = [("select keyword from search." + e + " where dsid = %(dsid)s "
                 "and vocabulary = 'GCMD'") for e in
                concept_map[concept]['db_tables'][0:-1]]
        union = " union ".join(ulst)
        q = ("select distinct g.path from (" + union + ") as p left join "
             "search." + concept_map[concept]['db_tables'][-1] + " as g on g."
             "uuid = p.keyword")
    else:
        q = ("select g.path from search." +
             concept_map[concept]['db_tables'][0] + " as v left join search."
             "gcmd_" + concept + " as g on g.uuid = v.keyword where v.dsid = "
             "%(dsid)s and v.vocabulary = 'GCMD'")

    cursor.execute(q, {'dsid': dsid})
    keywords = cursor.fetchall()
    if len(keywords) == 0:
        return

    md_keywords = (
            etree.SubElement(
                    etree.SubElement(
                            root,
                            "{" + nsmap['mri'] + "}descriptiveKeywords"),
                    "{" + nsmap['mri'] + "}MD_Keywords"))
    for keyword, in keywords:
        etree.SubElement(
                etree.SubElement(md_keywords, "{" + nsmap['mri'] + "}keyword"),
                "{" + nsmap['gco'] + "}CharacterString").text = keyword
        etree.SubElement(
                etree.SubElement(md_keywords, "{" + nsmap['mri'] + "}type"),
                "{" + nsmap['mri'] + "}MD_KeywordTypeCode",
                codeList=("http://standards.iso.org/iso/19115/resources/"
                          "Codelists/cat/codelists.xml#MD_KeywordTypeCode"),
                codeListValue="theme").text = "theme"
        ci_citation = (
                etree.SubElement(
                        etree.SubElement(
                                md_keywords,
                                "{" + nsmap['mri'] + "}thesaurusName"),
                        "{" + nsmap['cit'] + "}CI_Citation"))
        etree.SubElement(
                etree.SubElement(ci_citation, "{" + nsmap['cit'] + "}title"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                        "U.S. National Aeronautics and Space Administration "
                        "Global Change Master Directory")
        etree.SubElement(
                etree.SubElement(
                        ci_citation, "{" + nsmap['cit'] + "}alternateTitle"),
                "{" + nsmap['gco'] + "}CharacterString").text = "GCMD"
        etree.SubElement(
                etree.SubElement(ci_citation, "{" + nsmap['cit'] + "}edition"),
                "{" + nsmap['gco'] + "}CharacterString").text = edition.strip()
        etree.SubElement(
                etree.SubElement(
                        ci_citation,
                        "{" + nsmap['cit'] + "}otherCitationDetails"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                        "Valids List: " + concept_map[concept]['list_type'] +
                        " Keywords")


def add_constraints(root, nsmap, cursor, dsid):
    md_constraints = (
            etree.SubElement(
                    etree.SubElement(
                            root, "{" + nsmap['mri'] + "}resourceConstraints"),
                    "{" + nsmap['mco'] + "}MD_LegalConstraints"))
    cursor.execute(("select access_restrict from wagtail."
                    "dataset_description_datasetdescriptionpage "
                    "where dsid = %s"), (dsid, ))
    access = cursor.fetchone()
    if access is not None and len(access[0]) > 0:
        code = "otherRestrictions"
        access = convert_html_to_text("<access>" + access[0] + "</access>")
        etree.SubElement(
                etree.SubElement(
                        md_constraints, "{" + nsmap['mco'] + "}useLimitation"),
                "{" + nsmap['gco'] + "}CharacterString").text = access
    else:
        code = "unrestricted"

    etree.SubElement(
            etree.SubElement(
                    md_constraints, "{" + nsmap['mco'] + "}accessConstraints"),
            "{" + nsmap['mco'] + "}MD_RestrictionCode",
            codeList=("http://standards.iso.org/iso/19115/resources/Codelists/"
                      "cat/codelists.xml#MD_RestrictionCode"),
            codeListValue=code).text = code
    cursor.execute(("select usage_restrict from wagtail."
                    "dataset_description_datasetdescriptionpage "
                    "where dsid = %s"), (dsid, ))
    usage = cursor.fetchone()
    if usage is not None and len(usage[0]) > 0:
        md_constraints = (
                etree.SubElement(
                        etree.SubElement(
                                root,
                                "{" + nsmap['mri'] + "}resourceConstraints"),
                        "{" + nsmap['mco'] + "}MD_LegalConstraints"))
        etree.SubElement(
                etree.SubElement(
                        md_constraints,
                        "{" + nsmap['mco'] + "}accessConstraints"),
                "{" + nsmap['mco'] + "}MD_RestrictionCode",
                codeList=("http://standards.iso.org/iso/19115/resources/"
                          "Codelists/cat/codelists.xml#MD_RestrictionCode"),
                codeListValue="otherRestrictions").text = "otherRestrictions"


def add_associated_resources(root, nsmap, cursor, dsid):
    cursor.execute((
            "select related_rsrc_list from wagtail."
            "dataset_description_datasetdescriptionpage where dsid = %s"),
            (dsid, ))
    rsrcs = cursor.fetchone()
    if rsrcs is not None and len(rsrcs) > 0:
        for rsrc in rsrcs[0]:
            ci_citation = (
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        etree.SubElement(
                                                root,
                                                ("{" + nsmap['mri'] +
                                                 "}associatedResource")),
                                        ("{" + nsmap['mri'] +
                                         "}MD_AssociatedResource")),
                                "{" + nsmap['mri'] + "}name"),
                        "{" + nsmap['cit'] + "}CI_Citation"))
            etree.SubElement(
                    etree.SubElement(
                            ci_citation, "{" + nsmap['cit'] + "}title"),
                    "{" + nsmap['gco'] + "}CharacterString").text = (
                            rsrc['description'])
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(
                                    etree.SubElement(
                                            ci_citation,
                                            ("{" + nsmap['cit'] +
                                             "}onlineResource")),
                                    "{" + nsmap['cit'] + "}CI_OnlineResource"),
                            "{" + nsmap['cit'] + "}linkage"),
                    "{" + nsmap['gco'] + "}CharacterString").text = rsrc['url']


def add_data_identification(root, nsmap, mcursor, wcursor, dsid):
    mcursor.execute(("select title, summary, pub_date, continuing_update from "
                     "search.datasets where dsid = %s"), (dsid, ))
    title, abstract, pub_date, progress = mcursor.fetchone()
    abstract = convert_html_to_text(
            "<abstract>" + abstract + "</abstract>")
    progress = "onGoing" if progress == "Y" else "completed"
    data_ident = (
            etree.SubElement(
                    etree.SubElement(root, ("{" + nsmap['mdb'] +
                                            "}identificationInfo")),
                    "{" + nsmap['mri'] + "}MD_DataIdentification"))
    ci_citation = (
            etree.SubElement(
                    etree.SubElement(data_ident,
                                     "{" + nsmap['mri'] + "}citation"),
                    "{" + nsmap['cit'] + "}CI_Citation"))
    etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['cit'] + "}title"),
            "{" + nsmap['gco'] + "}CharacterString").text = title
    ci_date = (
            etree.SubElement(
                    etree.SubElement(ci_citation,
                                     "{" + nsmap['cit'] + "}date"),
                    "{" + nsmap['cit'] + "}CI_Date"))
    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['cit'] + "}date"),
            "{" + nsmap['gco'] + "}DateTime").text = (
                    str(pub_date) + "T00:00:00-06:00")
    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['cit'] + "}dateType"),
            "{" + nsmap['cit'] + "}CI_DateTypeCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.html#CI_DateTypeCode"),
            codeListValue="publication").text = "publication"
    mcursor.execute((
            "select doi from dssdb.dsvrsn where dsid = %s and status = 'A'"),
            (dsid, ))
    doi = mcursor.fetchone()
    if doi is not None:
        md_ident = (
                etree.SubElement(
                        etree.SubElement(ci_citation,
                                         "{" + nsmap['cit'] + "}identifier"),
                        "{" + nsmap['mcc'] + "}MD_Identifier"))
        etree.SubElement(
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(md_ident,
                                                 ("{" + nsmap['mcc'] +
                                                  "}authority")),
                                "{" + nsmap['cit'] + "}CI_Citation"),
                        "{" + nsmap['cit'] + "}title"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                        "International DOI Foundation")
        etree.SubElement(
                etree.SubElement(md_ident, "{" + nsmap['mcc'] + "}code"),
                "{" + nsmap['gco'] + "}CharacterString").text = doi[0]
        etree.SubElement(
                etree.SubElement(md_ident, "{" + nsmap['mcc'] + "}codeSpace"),
                "{" + nsmap['gco'] + "}CharacterString").text = "doi"

    add_authors(ci_citation, nsmap, mcursor, dsid)
    etree.SubElement(
            etree.SubElement(data_ident, "{" + nsmap['mri'] + "}abstract"),
            "{" + nsmap['gco'] + "}CharacterString").text = abstract
    etree.SubElement(
            etree.SubElement(data_ident, "{" + nsmap['mri'] + "}status"),
            "{" + nsmap['mcc'] + "}MD_ProgressCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#MD_ProgressCode"),
            codeListValue=progress).text = progress
    ci_respons = (
            etree.SubElement(
                    etree.SubElement(data_ident,
                                     "{" + nsmap['mri'] + "}pointOfContact"),
                    "{" + nsmap['cit'] + "}CI_Responsibility"))
    etree.SubElement(
            etree.SubElement(ci_respons, "{" + nsmap['cit'] + "}role"),
            "{" + nsmap['cit'] + "}CI_RoleCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#CI_RoleCode"),
            codeListValue="pointOfContact").text = "pointOfContact"
    ci_org = (
            etree.SubElement(
                    etree.SubElement(ci_respons,
                                     "{" + nsmap['cit'] + "}party"),
                    "{" + nsmap['cit'] + "}CI_Organisation"))
    etree.SubElement(
            etree.SubElement(ci_org, "{" + nsmap['cit'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['name'])
    etree.SubElement(
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(
                                    etree.SubElement(
                                            etree.SubElement(
                                                    ci_org,
                                                    ("{" + nsmap['cit'] +
                                                     "}contactInfo")),
                                            ("{" + nsmap['cit'] +
                                             "}CI_Contact")),
                                    "{" + nsmap['cit'] + "}address"),
                            "{" + nsmap['cit'] + "}CI_Address"),
                    "{" + nsmap['cit'] + "}electronicMailAddress"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['email'])
    geoext = fill_geographic_extent_data(dsid, mcursor)
    if 'is_grid' in geoext and geoext['is_grid']:
        etree.SubElement(
                etree.SubElement(data_ident,
                                 ("{" + nsmap['mri'] +
                                  "}spatialRepresentationType")),
                "{" + nsmap['mcc'] + "}MD_SpatialRepresentationTypeCode",
                codeList=("http://standards.iso.org/iso/19115/resources/"
                          "Codelists/cat/codelists.xml#"
                          "MD_SpatialRepresentationTypeCode"),
                codeListValue="grid").text = "grid"

    if 'hres' in geoext:
        for val in geoext['hres'].values():
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(
                                    etree.SubElement(
                                            data_ident,
                                            ("{" + nsmap['mri'] +
                                             "}spatialResolution")),
                                    "{" + nsmap['mri'] + "}MD_Resolution"),
                            "{" + nsmap['mri'] + "}distance"),
                    "{" + nsmap['gco'] + "}Distance",
                    uom=val['uom']).text = val['dist']

    add_extent(data_ident, nsmap, mcursor, dsid, geoext)
    add_references(data_ident, nsmap)
    if progress == "onGoing":
        add_maint_frequency(data_ident, nsmap, wcursor, dsid)

    wcursor.execute((
            "select dslogo from wagtail."
            "dataset_description_datasetdescriptionpage where dsid = %s"),
            (dsid, ))
    logo = wcursor.fetchone()
    if logo is not None:
        logo = logo[0]
        if len(logo) > 0 and logo.find("default") != 0:
            add_graphic_overview(data_ident, nsmap, logo)

    add_data_formats(data_ident, nsmap, mcursor, dsid)
    add_gcmd_keywords(data_ident, nsmap, mcursor, dsid, "sciencekeywords")
    add_gcmd_keywords(data_ident, nsmap, mcursor, dsid, "platforms")
    add_gcmd_keywords(data_ident, nsmap, mcursor, dsid, "projects")
    add_gcmd_keywords(data_ident, nsmap, mcursor, dsid, "instruments")
    add_constraints(data_ident, nsmap, wcursor, dsid)
    add_associated_resources(data_ident, nsmap, wcursor, dsid)


def add_distribution_info(root, nsmap, dsid):
    md_distrib = (
            etree.SubElement(
                    etree.SubElement(
                            root, "{" + nsmap['mdb'] + "}distributionInfo"),
                    "{" + nsmap['mrd'] + "}MD_Distribution"))
    ci_respons = (
        etree.SubElement(
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        md_distrib,
                                        "{" + nsmap['mrd'] + "}distributor"),
                                "{" + nsmap['mrd'] + "}MD_Distributor"),
                        "{" + nsmap['mrd'] + "}distributorContact"),
                "{" + nsmap['cit'] + "}CI_Responsibility"))
    etree.SubElement(
            etree.SubElement(ci_respons, "{" + nsmap['cit'] + "}role"),
            "{" + nsmap['cit'] + "}CI_RoleCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#CI_RoleCode"),
            codeListValue="distributor").text = "distributor"
    ci_org = (
            etree.SubElement(
                    etree.SubElement(
                            ci_respons, "{" + nsmap['cit'] + "}party"),
                    "{" + nsmap['cit'] + "}CI_Organisation"))
    etree.SubElement(
            etree.SubElement(ci_org, "{" + nsmap['cit'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    settings.ARCHIVE['name'])
    ci_address = (
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(
                                    etree.SubElement(
                                            ci_org,
                                            ("{" + nsmap['cit'] +
                                             "}contactInfo")),
                                    "{" + nsmap['cit'] + "}CI_Contact"),
                            "{" + nsmap['cit'] + "}address"),
                    "{" + nsmap['cit'] + "}CI_Address"))
    etree.SubElement(
            etree.SubElement(
                    ci_address,
                    "{" + nsmap['cit'] + "}electronicMailAddress"),
            "{" + nsmap['gco'] + "}CharacterString").text = "rdahelp@ucar.edu"
    ci_online = (
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(
                                    etree.SubElement(
                                            md_distrib,
                                            ("{" + nsmap['mrd'] +
                                             "}transferOptions")),
                                    ("{" + nsmap['mrd'] +
                                     "}MD_DigitalTransferOptions")),
                            "{" + nsmap['mrd'] + "}onLine"),
                    "{" + nsmap['cit'] + "}CI_OnlineResource"))
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}linkage"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    os.path.join(settings.ARCHIVE['url'], "datasets", "dsid"))
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}protocol"),
            "{" + nsmap['gco'] + "}CharacterString").text = "https"
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                   "Data Access for " + dsid)
    etree.SubElement(
            etree.SubElement(ci_online, "{" + nsmap['cit'] + "}function"),
            "{" + nsmap['cit'] + "}CI_OnLineFunctionCode",
            codeList=("http://standards.iso.org/iso/19115/-3/cit/1.0/"
                      "codelists.xml#CI_OnLineFunctionCode"),
            codeListValue="download").text = "download"


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
        add_metadata_date(root, nsmap, mcursor, dsid)
        add_metadata_standard(root, nsmap)
        add_alt_metadata_ref(root, nsmap)
        add_metadata_linkage(root, nsmap, dsid)
        add_data_identification(root, nsmap, mcursor, wcursor, dsid)
        add_distribution_info(root, nsmap, dsid)
    finally:
        mconn.close()
        wconn.close()

    return etree.tostring(root, pretty_print=True).decode("utf-8")
