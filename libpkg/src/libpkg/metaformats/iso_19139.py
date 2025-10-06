import os
import psycopg2

from datetime import datetime, timedelta
from lxml import etree

from . import settings
from ..geospatial import fill_geographic_extent_data
from ..metautils import (get_dataset_size,
                         get_date_from_precision,
                         open_dataset_overview)
from ..strutils import snake_to_capital
from ..xmlutils import convert_html_to_text


def metadata_date(dsid, d1, cursor):
    d2 = datetime(1000, 1, 1, 0, 0, 0)
    try:
        cursor.execute((
                "select max(date_created + time_created) from dssdb.wfile_" +
                dsid))
        d2 = cursor.fetchone()[0] + timedelta(hours=6)
    except Exception:
        pass

    return max(d1, d2)


def add_file_identifier(root, nsmap, dsid):
    parts = settings.ARCHIVE['domain'].split(".")
    etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}fileIdentifier"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            ".".join(reversed(parts)) + "::" + dsid)


def add_language(root, nsmap):
    etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}language"),
            "{" + nsmap['gmd'] + "}LanguageCode",
            codeList="http://www.loc.gov/standards/iso639-2/",
            codeListValue="eng; USA").text = "eng; USA"


def add_charset(root, nsmap):
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}characterSet"),
            "{" + nsmap['gmd'] + "}MD_CharacterSetCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_CharacterSetCode"),
            codeListValue="utf8").text = "utf-8"


def add_hierarchy_level(root, nsmap):
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}hierarchyLevel"),
            "{" + nsmap['gmd'] + "}MD_ScopeCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_ScopeCode"),
            codeListValue="dataset").text = "dataset"


def add_contact(root, nsmap):
    contact = etree.SubElement(root, "{" + nsmap['gmd'] + "}contact")
    ci_responsibleparty = etree.SubElement(
            contact, "{" + nsmap['gmd'] + "}CI_ResponsibleParty")
    etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty,
                    "{" + nsmap['gmd'] + "}organisationName"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            settings.ARCHIVE['name'])
    ci_contact = etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty, "{" + nsmap['gmd'] + "}contactInfo"),
            "{" + nsmap['gmd'] + "}CI_Contact")
    ci_onlineresource = etree.SubElement(
            etree.SubElement(
                    ci_contact, "{" + nsmap['gmd'] + "}onlineResource"),
            "{" + nsmap['gmd'] + "}CI_OnlineResource")
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}linkage"),
            "{" + nsmap['gmd'] + "}URL").text = (
                    os.path.join("https://", settings.ARCHIVE['domain']))
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            settings.ARCHIVE['name'])
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource,
                    "{" + nsmap['gmd'] + "}description"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            settings.ARCHIVE['description'])
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}function"),
            "{" + nsmap['gmd'] + "}CI_OnLineFunctionCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_OnLineFunctionCode"),
            codeListValue="download").text = "download"
    etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty, "{" + nsmap['gmd'] + "}role"),
            "{" + nsmap['gmd'] + "}CI_RoleCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_RoleCode"),
            codeListValue="pointOfContact").text = "pointOfContact"


def add_date_stamp(root, nsmap, tstamp):
    etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}dateStamp"),
            "{" + nsmap['gco'] + "}DateTime").text = tstamp


def add_metadata_standard(root, nsmap):
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}metadataStandardName"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            "ISO 19115 Geographic Information - Metadata")
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}metadataStandardVersion"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            "ISO19115:2003/Cor 1 2006")


def add_dataset_uri(root, nsmap, dsid, cursor):
    cursor.execute(("select doi from dssdb.dsvrsn where dsid = %s and status "
                    "= 'A'"), (dsid, ))
    res = cursor.fetchone()
    if res is not None:
        ds_uri = os.path.join(settings.DOI_DOMAIN, res[0])
    else:
        ds_uri = os.path.join("https://", settings.ARCHIVE['domain'],
                              settings.ARCHIVE['datasets_path'], dsid)

    etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}dataSetURI"),
            "{" + nsmap['gco'] + "}CharacterString").text = ds_uri


def add_related_resources(root, nsmap, related_resources):
    for x in range(0, len(related_resources)):
        ci_onlineresource = etree.SubElement(
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        root, ("{" + nsmap['gmd'] +
                                               "}metadataExtensionInfo")),
                                ("{" + nsmap['gmd'] +
                                 "}MD_MetadataExtensionInformation")),
                        "{" + nsmap['gmd'] + "}extensionOnLineResource"),
                "{" + nsmap['gmd'] + "}CI_OnlineResource")
        url = related_resources[x].get("url")
        etree.SubElement(
                etree.SubElement(
                        ci_onlineresource,
                        "{" + nsmap['gmd'] + "}linkage"),
                "{" + nsmap['gmd'] + "}URL").text = url
        etree.SubElement(
                etree.SubElement(
                        ci_onlineresource,
                        "{" + nsmap['gmd'] + "}protocol"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                url[0:url.find("://")])
        etree.SubElement(
                etree.SubElement(
                        ci_onlineresource, "{" + nsmap['gmd'] + "}name"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                "Related Resource #" + str(x+1))
        etree.SubElement(
                etree.SubElement(
                        ci_onlineresource,
                        "{" + nsmap['gmd'] + "}description"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                related_resources[x].text)


def add_di_citation(root, nsmap, nil_reason, dsid, authors, cursor, title,
                    pub_date):
    ci_citation = etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}citation"),
            "{" + nsmap['gmd'] + "}CI_Citation")
    etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['gmd'] + "}title"),
            "{" + nsmap['gco'] + "}CharacterString").text = title
    etree.SubElement(
            etree.SubElement(
                    ci_citation, "{" + nsmap['gmd'] + "}alternateTitle"),
            "{" + nsmap['gco'] + "}CharacterString").text = dsid
    ci_date = etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['gmd'] + "}date"),
            "{" + nsmap['gmd'] + "}CI_Date")
    pub_date = str(pub_date)
    if pub_date < "9999-01-01":
        etree.SubElement(
                etree.SubElement(ci_date, "{" + nsmap['gmd'] + "}date"),
                "{" + nsmap['gco'] + "}Date").text = pub_date
    else:
        etree.SubElement(ci_date, "{" + nsmap['gmd'] + "}date",
                         {nil_reason: "unknown"}, nsmap=nsmap)

    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['gmd'] + "}dateType"),
            "{" + nsmap['gmd'] + "}CI_DateTypeCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_DateTypeCode"),
            codeListValue="publication").text = "publication"
    author_list = []
    for author in authors:
        auth_type = author.get("{" + nsmap['xsi'] + "}type")
        if auth_type is None or auth_type == "authorPerson":
            author_list.append({
                'type': "P",
                'name': (author.get("lname") + ", " + author.get("fname") + " "
                         + author.get("mname")).strip(),
                'title': ((author.get("fname") + " " +
                          author.get("mname")).strip() + " " +
                          author.get("lname"))})
            orcid_id = author.get("orcid_id")
            if orcid_id is not None:
                author_list[-1].update({'orcid_id': orcid_id})

        else:
            author_list.append({'type': "O", 'name': author.get("name")})

    if len(author_list) == 0:
        cursor.execute(("select g.path from search.contributors_new as c "
                        "left join search.gcmd_providers as g on g.uuid = c."
                        "keyword where c.dsid = %s and c.vocabulary "
                        "= 'GCMD'"), (dsid, ))
        res = cursor.fetchall()
        if len(res) == 0:
            raise RuntimeError(("no authors or contributors could be "
                                "identified"))

        for e in res:
            author_list.append({
                'type': "O",
                'name': e[0][(e[0].find(" > ")+3):]})

    for author in author_list:
        ci_responsibleparty = etree.SubElement(
                etree.SubElement(
                        ci_citation,
                        "{" + nsmap['gmd'] + "}citedResponsibleParty"),
                "{" + nsmap['gmd'] + "}CI_ResponsibleParty")
        if author['type'] == "P":
            individual_name = etree.SubElement(
                    ci_responsibleparty,
                    "{" + nsmap['gmd'] + "}individualName")
            if 'orcid_id' in author:
                anchor = etree.SubElement(
                        individual_name,
                        "{" + nsmap['gmx'] + "}Anchor")
                anchor.set("{" + nsmap['xlink'] + "}href",
                           "http://orcid.org/" + author['orcid_id'])
                anchor.set("{" + nsmap['xlink'] + "}title", author['title'])
                anchor.set("{" + nsmap['xlink'] + "}actuate", "onRequest")
                anchor.text = author['name']
            else:
                etree.SubElement(
                        individual_name,
                        "{" + nsmap['gco'] + "}CharacterString").text = (
                        author['name'])

        else:
            etree.SubElement(
                    etree.SubElement(
                            ci_responsibleparty,
                            "{" + nsmap['gmd'] + "}organisationName"),
                    "{" + nsmap['gco'] + "}CharacterString").text = (
                    author['name'])

        etree.SubElement(
                etree.SubElement(
                        ci_responsibleparty,
                        "{" + nsmap['gmd'] + "}role"),
                "{" + nsmap['gmd'] + "}CI_RoleCode",
                codeList=("http://www.isotc211.org/2005/resources/"
                          "Codelist/gmxCodelists.xml#CI_RoleCode"),
                codeListValue="author").text = "author"

    ci_responsibleparty = etree.SubElement(
            etree.SubElement(
                    ci_citation,
                    "{" + nsmap['gmd'] + "}citedResponsibleParty"),
            "{" + nsmap['gmd'] + "}CI_ResponsibleParty")
    etree.SubElement(
            etree.SubElement(ci_responsibleparty,
                             "{" + nsmap['gmd'] + "}organisationName"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            settings.ARCHIVE['pub_name']['default'])
    etree.SubElement(
            etree.SubElement(ci_responsibleparty,
                             "{" + nsmap['gmd'] + "}role"),
            "{" + nsmap['gmd'] + "}CI_RoleCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_RoleCode"),
            codeListValue="publisher").text = "publisher"


def add_di_abstract(root, nsmap, abstract):
    etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}abstract"),
            "{" + nsmap['gco'] + "}CharacterString").text = abstract


def add_di_status(root, nsmap, progress):
    etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}status"),
            "{" + nsmap['gmd'] + "}MD_ProgressCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_ProgressCode"),
            codeListValue=progress).text = progress


def add_di_point_of_contact(root, nsmap):
    ci_responsibleparty = etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}pointOfContact"),
            "{" + nsmap['gmd'] + "}CI_ResponsibleParty")
    ci_contact = etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty,
                    "{" + nsmap['gmd'] + "}contactInfo"),
            "{" + nsmap['gmd'] + "}CI_Contact")
    ci_address = etree.SubElement(
            etree.SubElement(ci_contact, "{" + nsmap['gmd'] + "}address"),
            "{" + nsmap['gmd'] + "}CI_Address")
    etree.SubElement(
            etree.SubElement(
                    ci_address,
                    "{" + nsmap['gmd'] + "}electronicMailAddress"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            settings.ARCHIVE['email'])
    etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty, "{" + nsmap['gmd'] + "}role"),
            "{" + nsmap['gmd'] + "}CI_RoleCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_RoleCode"),
            codeListValue="pointOfContact").text = "pointOfContact"


def add_di_resource_maintenance(root, nsmap, frequency):
    md_maintenanceinformation = etree.SubElement(
            etree.SubElement(root,
                             "{" + nsmap['gmd'] + "}resourceMaintenance"),
            "{" + nsmap['gmd'] + "}MD_MaintenanceInformation")
    if frequency is not None:
        if frequency == "bi_monthly":
            frequency = "monthly"
        elif frequency == "half-yearly":
            frequency = "biannually"
        elif frequency == "yearly":
            frequency = "annually"
        elif frequency == "irregularly":
            frequency = "irregular"

    else:
        frequency = "notPlanned"

    etree.SubElement(
            etree.SubElement(
                    md_maintenanceinformation,
                    "{" + nsmap['gmd'] + "}maintenanceAndUpdateFrequency"),
            "{" + nsmap['gmd'] + "}MD_MaintenanceFrequencyCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_MaintenanceFrequencyCode"),
            codeListValue=frequency).text = frequency
    etree.SubElement(
            etree.SubElement(
                    md_maintenanceinformation,
                    "{" + nsmap['gmd'] + "}updateScope"),
            "{" + nsmap['gmd'] + "}MD_ScopeCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_ScopeCode"),
            codeListValue="dataset").text = "dataset"


def add_di_graphic_overview(root, nsmap, logo):
    md_browsegraphic = etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}graphicOverview"),
            "{" + nsmap['gmd'] + "}MD_BrowseGraphic")
    etree.SubElement(
            etree.SubElement(
                    md_browsegraphic, "{" + nsmap['gmd'] + "}fileName"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
                    os.path.join("https://", settings.ARCHIVE['domain'],
                                 "images/ds_logos", logo.text))
    etree.SubElement(
            etree.SubElement(
                    md_browsegraphic, "{" + nsmap['gmd'] + "}fileDescription"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            "Logo thumbnail from URL")
    etree.SubElement(
            etree.SubElement(
                    md_browsegraphic, "{" + nsmap['gmd'] + "}fileType"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            logo.text[(logo.text.find(".")+1):])


def add_di_resource_formats(root, nsmap, nil_reason, formats):
    for e in formats:
        md_format = etree.SubElement(
                etree.SubElement(root, "{" + nsmap['gmd'] + "}resourceFormat"),
                "{" + nsmap['gmd'] + "}MD_Format")
        etree.SubElement(
                etree.SubElement(
                        md_format, "{" + nsmap['gmd'] + "}name"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                snake_to_capital(e[0].replace("proprietary_", "")))
        etree.SubElement(md_format, "{" + nsmap['gmd'] + "}version",
                         {nil_reason: "inapplicable"},
                         nsmap=nsmap)


def add_di_keywords(root, nsmap, keywords, **kwargs):
    md_keywords = etree.SubElement(
            etree.SubElement(root,
                             "{" + nsmap['gmd'] + "}descriptiveKeywords"),
            "{" + nsmap['gmd'] + "}MD_Keywords")
    for keyword in keywords:
        etree.SubElement(
                etree.SubElement(md_keywords, "{" + nsmap['gmd'] + "}keyword"),
                "{" + nsmap['gco'] + "}CharacterString").text = keyword

    ci_citation = etree.SubElement(
            etree.SubElement(
                    md_keywords, "{" + nsmap['gmd'] + "}thesaurusName"),
            "{" + nsmap['gmd'] + "}CI_Citation")
    etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['gmd'] + "}title"),
            "{" + nsmap['gco'] + "}CharacterString").text = kwargs['title']
    if 'alternateTitle' in kwargs:
        etree.SubElement(
                etree.SubElement(ci_citation,
                                 "{" + nsmap['gmd'] + "}alternateTitle"),
                "{" + nsmap['gco'] + "}CharacterString").text = (
                kwargs['alternateTitle'])

    ci_date = etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['gmd'] + "}date"),
            "{" + nsmap['gmd'] + "}CI_Date")
    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['gmd'] + "}date"),
            "{" + nsmap['gco'] + "}Date").text = kwargs['revisionDate']
    etree.SubElement(
            etree.SubElement(ci_date, "{" + nsmap['gmd'] + "}dateType"),
            "{" + nsmap['gmd'] + "}CI_DateTypeCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_DateTypeCode"),
            codeListValue="revision").text = "revision"
    etree.SubElement(
            etree.SubElement(ci_citation, "{" + nsmap['gmd'] + "}edition"),
            "{" + nsmap['gco'] + "}CharacterString").text = kwargs['edition']
    ci_responsibleparty = etree.SubElement(
            etree.SubElement(
                    ci_citation,
                    "{" + nsmap['gmd'] + "}citedResponsibleParty"),
            "{" + nsmap['gmd'] + "}CI_ResponsibleParty")
    etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty,
                    "{" + nsmap['gmd'] + "}organisationName"),
            "{" + nsmap['gco'] + "}CharacterString").text = kwargs['orgName']
    etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty,
                    "{" + nsmap['gmd'] + "}role"),
            "{" + nsmap['gmd'] + "}CI_RoleCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_RoleCode"),
            codeListValue="originator").text = "orginator"
    if 'otherDetails' in kwargs:
        etree.SubElement(
                    etree.SubElement(
                        ci_citation,
                        "{" + nsmap['gmd'] + "}otherCitationDetails"),
                    "{" + nsmap['gco'] + "}CharacterString").text = (
                    kwargs['otherDetails'])


def add_di_resource_constraints(root, nsmap, dsid, cursor):
    md_legalconstraints = etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}resourceConstraints"),
            "{" + nsmap['gmd'] + "}MD_LegalConstraints")
    cursor.execute(("select usage_restrict from wagtail2."
                    "dataset_description_datasetdescriptionpage where "
                    "dsid = %s"), (dsid, ))
    usage = cursor.fetchone()
    if usage is not None and len(usage[0]) > 0:
        usage = convert_html_to_text("<usage>" + usage[0] + "</usage>")
    else:
        cursor.execute(("select data_license from wagtail2."
                        "dataset_description_datasetdescriptionpage "
                        "where dsid = %s"), (dsid, ))
        usage = cursor.fetchone()[0]['name']

    etree.SubElement(
            etree.SubElement(
                    md_legalconstraints,
                    "{" + nsmap['gmd'] + "}useLimitation"),
            "{" + nsmap['gco'] + "}CharacterString").text = usage
    cursor.execute(("select access_restrict from wagtail2."
                    "dataset_description_datasetdescriptionpage "
                    "where dsid = %s"), (dsid, ))
    access = cursor.fetchone()
    if access is None or len(access[0]) == 0:
        access = "None"
    else:
        access = convert_html_to_text("<access>" + access[0] + "</access>")

    etree.SubElement(
            etree.SubElement(
                    md_legalconstraints,
                    "{" + nsmap['gmd'] + "}otherConstraints"),
            "{" + nsmap['gco'] + "}CharacterString").text = access


def add_di_spatial_representation_type(root, nsmap, stype):
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}spatialRepresentationType"),
            "{" + nsmap['gmd'] + "}MD_SpatialRepresentationTypeCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_SpatialRepresentationTypeCode"),
            codeListValue=stype).text = stype


def add_di_spatial_resolution(root, nsmap, hres):
    for val in hres.values():
        etree.SubElement(
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        root,
                                        ("{" + nsmap['gmd'] +
                                         "}spatialResolution")),
                                "{" + nsmap['gmd'] + "}MD_Resolution"),
                        "{" + nsmap['gmd'] + "}distance"),
                "{" + nsmap['gco'] + "}Distance",
                uom=val['uom']).text = val['dist']


def add_di_language(root, nsmap):
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}language"),
            "{" + nsmap['gco'] + "}CharacterString").text = "eng"


def add_di_charset(root, nsmap):
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}characterSet"),
            "{" + nsmap['gmd'] + "}MD_CharacterSetCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_CharacterSetCode"),
            codeListValue="utf8").text = "utf-8"


def add_di_topic_category(root, nsmap, topic):
    etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}topicCategory"),
            "{" + nsmap['gmd'] + "}MD_TopicCategoryCode").text = topic


def get_di_temporal_extent(dsid, cursor):
    cursor.execute(("select min(concat(date_start, ' ', time_start)), min("
                    "start_flag), max(concat(date_end, ' ', time_end)), min("
                    "end_flag), min(time_zone) from dssdb.dsperiod where dsid "
                    "= %s and date_start < '9998-01-01' and date_end < "
                    "'9998-01-01'"), (dsid, ))
    res = cursor.fetchone()
    if res is None or not all(res):
        return (False, None, None)

    tz = res[4]
    idx = tz.find(",")
    if idx > 0:
        tz = tz[0:idx]

    return (True, get_date_from_precision(res[0], res[1], tz),
            get_date_from_precision(res[2], res[3], tz))


def add_di_extent(root, nsmap, dsid, geoext, tempext):
    extents = {'point': False, 'box': False, 'location': False,
               'temporal': False}
    extents['temporal'], begin_date, end_date = tempext
    if all(geoext.values()):
        if 'wlon' in geoext:
            if (geoext['wlon'] == geoext['elon'] and geoext['slat'] ==
                    geoext['nlat']):
                extents['point'] = True
            else:
                extents['box'] = True

    if not any(extents.values()):
        return

    ex_extent = etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}extent"),
            "{" + nsmap['gmd'] + "}EX_Extent")
    id = etree.QName(nsmap['gml'], "id")
    if extents['point'] or extents['box'] or extents['location']:
        geographicelement = etree.SubElement(
                ex_extent, "{" + nsmap['gmd'] + "}geographicElement")

    if extents['point']:
        etree.SubElement(
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        geographicelement,
                                        ("{" + nsmap['gmd'] +
                                         "}EX_BoundingPolygon")),
                                "{" + nsmap['gmd'] + "}polygon"),
                        "{" + nsmap['gml'] + "}Point",
                        {id: dsid + "_point"}, nsmap=nsmap),
                "{" + nsmap['gml'] + "}pos").text = (
                " ".join([str(geoext['wlon']), str(geoext['slat'])]))

    if extents['box']:
        box = etree.SubElement(
                geographicelement,
                "{" + nsmap['gmd'] + "}EX_GeographicBoundingBox")
        etree.SubElement(
                etree.SubElement(
                        box, "{" + nsmap['gmd'] + "}westBoundLongitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['wlon'])
        etree.SubElement(
                etree.SubElement(
                        box, "{" + nsmap['gmd'] + "}eastBoundLongitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['elon'])
        etree.SubElement(
                etree.SubElement(
                        box, "{" + nsmap['gmd'] + "}southBoundLatitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['slat'])
        etree.SubElement(
                etree.SubElement(
                        box, "{" + nsmap['gmd'] + "}northBoundLatitude"),
                "{" + nsmap['gco'] + "}Decimal").text = str(geoext['nlat'])

    if extents['temporal']:
        timeperiod = etree.SubElement(
                etree.SubElement(
                        etree.SubElement(
                                etree.SubElement(
                                        ex_extent,
                                        ("{" + nsmap['gmd'] +
                                         "}temporalElement")),
                                "{" + nsmap['gmd'] + "}EX_TemporalExtent"),
                        "{" + nsmap['gmd'] + "}extent"),
                "{" + nsmap['gml'] + "}TimePeriod",
                {id: dsid + "_time_period"}, nsmap=nsmap)
        etree.SubElement(
                timeperiod,
                "{" + nsmap['gml'] + "}beginPosition").text = begin_date
        etree.SubElement(
                timeperiod,
                "{" + nsmap['gml'] + "}endPosition").text = end_date


def add_data_identification(root, nsmap, nil_reason, dsid, mcursor, wcursor,
                            xml_root):
    md_dataidentification = etree.SubElement(
            etree.SubElement(
                    root, "{" + nsmap['gmd'] + "}identificationInfo"),
            "{" + nsmap['gmd'] + "}MD_DataIdentification")
    mcursor.execute(("select title, summary, pub_date, continuing_update from "
                     "search.datasets where dsid = %s"), (dsid, ))
    title, abstract, pub_date, progress = mcursor.fetchone()
    add_di_citation(md_dataidentification, nsmap, nil_reason, dsid,
                    xml_root.findall("./author"), mcursor, title, pub_date)
    add_di_abstract(md_dataidentification, nsmap, abstract)
    add_di_status(md_dataidentification, nsmap, progress)
    add_di_point_of_contact(md_dataidentification, nsmap)
    add_di_resource_maintenance(
            md_dataidentification, nsmap,
            xml_root.find("./continuingUpdate").get("frequency"))
    logo = xml_root.find("./logo")
    if logo is not None:
        add_di_graphic_overview(md_dataidentification, nsmap, logo)

    mcursor.execute(("select distinct keyword from search.formats where "
                     "dsid = %s"), (dsid, ))
    formats = mcursor.fetchall()
    add_di_resource_formats(md_dataidentification, nsmap, nil_reason, formats)
    # DataCite resource type
    add_di_keywords(md_dataidentification, nsmap,
                    ["dataset"],
                    title="Resource Type",
                    revisionDate="2021-03-30",
                    edition="4.4",
                    orgName="DataCite Metadata Working Group",
                    otherDetails="resourceTypeGeneral")
    mcursor.execute(("select g.path from search.platforms_new as p left "
                     "join search.gcmd_platforms as g on g.uuid = p."
                     "keyword where dsid = %s and p.vocabulary = 'GCMD'"),
                    (dsid, ))
    platforms = mcursor.fetchall()
    if len(platforms) > 0:
        mcursor.execute(("select revision_date, version from search."
                         "gcmd_versions where concept_scheme = "
                         "'platforms'"))
        res = mcursor.fetchone()
        add_di_keywords(md_dataidentification, nsmap,
                        [e[0] for e in platforms],
                        title=settings.GCMD['title'],
                        alternateTitle=settings.GCMD['alternate_title'],
                        revisionDate=res[0],
                        edition=res[1].strip(),
                        orgName=settings.GCMD['org_name'],
                        otherDetails="Valids List: Platforms")

    mcursor.execute(("select g.path from search.instruments as i left "
                     "join search.gcmd_instruments as g on g.uuid = i."
                     "keyword where i.dsid = %s and i.vocabulary = "
                     "'GCMD'"), (dsid, ))
    instrs = mcursor.fetchall()
    if len(instrs) > 0:
        mcursor.execute(("select revision_date, version from search."
                         "gcmd_versions where concept_scheme = "
                         "'instruments'"))
        res = mcursor.fetchone()
        add_di_keywords(md_dataidentification, nsmap,
                        [e[0] for e in instrs],
                        title=settings.GCMD['title'],
                        alternateTitle=settings.GCMD['alternate_title'],
                        revisionDate=res[0],
                        edition=res[1].strip(),
                        orgName=settings.GCMD['org_name'],
                        otherDetails="Valids List: Instruments")

    mcursor.execute(("select distinct g.path from (select keyword from "
                     "search.projects_new where dsid = %s and vocabulary "
                     "= 'GCMD' union select keyword from search."
                     "supported_projects where dsid = %s  and vocabulary "
                     "= 'GCMD') as p left join search.gcmd_projects as g "
                     "on g.uuid = p.keyword"), (dsid, dsid))
    projects = mcursor.fetchall()
    if len(projects) > 0:
        mcursor.execute(("select revision_date, version from search."
                         "gcmd_versions where concept_scheme = "
                         "'projects'"))
        res = mcursor.fetchone()
        add_di_keywords(md_dataidentification, nsmap,
                        [e[0] for e in projects],
                        title=settings.GCMD['title'],
                        alternateTitle=settings.GCMD['alternate_title'],
                        revisionDate=res[0],
                        edition=res[1].strip(),
                        orgName=settings.GCMD['org_name'],
                        otherDetails="Valids List: Projects")

    mcursor.execute(("select g.path from search.variables as v left join "
                     "search.gcmd_sciencekeywords as g on g.uuid = v."
                     "keyword where v.dsid = %s and v.vocabulary = "
                     "'GCMD'"), (dsid, ))
    vars = mcursor.fetchall()
    mcursor.execute(("select revision_date, version from search."
                     "gcmd_versions where concept_scheme = "
                     "'sciencekeywords'"))
    res = mcursor.fetchone()
    add_di_keywords(md_dataidentification, nsmap,
                    [e[0] for e in vars],
                    title=settings.GCMD['title'],
                    alternateTitle=settings.GCMD['alternate_title'],
                    revisionDate=res[0],
                    edition=res[1].strip(),
                    orgName=settings.GCMD['org_name'],
                    otherDetails="Valids List: Science and Services Keywords")

    add_di_resource_constraints(md_dataidentification, nsmap, dsid, wcursor)
    geoext = fill_geographic_extent_data(dsid, mcursor)
    if 'is_grid' in geoext and geoext['is_grid']:
        add_di_spatial_representation_type(
                md_dataidentification, nsmap, "grid")

    if 'hres' in geoext:
        add_di_spatial_resolution(md_dataidentification, nsmap, geoext['hres'])

    add_di_language(md_dataidentification, nsmap)
    add_di_charset(md_dataidentification, nsmap)
    mcursor.execute(("select keyword from search.topics where dsid = %s "
                     "and vocabulary = 'ISO'"), (dsid, ))
    res = mcursor.fetchone()
    add_di_topic_category(md_dataidentification, nsmap, res[0])
    tempext = get_di_temporal_extent(dsid, mcursor)
    add_di_extent(md_dataidentification, nsmap, dsid, geoext, tempext)


def add_distribution_info(root, nsmap, dsid, size):
    md_distribution = etree.SubElement(
            etree.SubElement(root, "{" + nsmap['gmd'] + "}distributionInfo"),
            "{" + nsmap['gmd'] + "}MD_Distribution")
    md_distributor = etree.SubElement(
            etree.SubElement(
                    md_distribution, "{" + nsmap['gmd'] + "}distributor"),
            "{" + nsmap['gmd'] + "}MD_Distributor")
    ci_responsibleparty = etree.SubElement(
            etree.SubElement(
                    md_distributor,
                    "{" + nsmap['gmd'] + "}distributorContact"),
            "{" + nsmap['gmd'] + "}CI_ResponsibleParty")
    etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty,
                    "{" + nsmap['gmd'] + "}organisationName"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            settings.ARCHIVE['name'])
    ci_contact = etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty, "{" + nsmap['gmd'] + "}contactInfo"),
            "{" + nsmap['gmd'] + "}CI_Contact")
    ci_address = etree.SubElement(
            etree.SubElement(ci_contact, "{" + nsmap['gmd'] + "}address"),
            "{" + nsmap['gmd'] + "}CI_Address")
    etree.SubElement(
            etree.SubElement(
                    ci_address,
                    "{" + nsmap['gmd'] + "}electronicMailAddress"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            settings.ARCHIVE['email'])
    ci_onlineresource = etree.SubElement(
            etree.SubElement(
                    ci_contact, "{" + nsmap['gmd'] + "}onlineResource"),
            "{" + nsmap['gmd'] + "}CI_OnlineResource")
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}linkage"),
            "{" + nsmap['gmd'] + "}URL").text = (
                    os.path.join("https://", settings.ARCHIVE['domain']))
    etree.SubElement(
            etree.SubElement(
                    ci_responsibleparty, "{" + nsmap['gmd'] + "}role"),
            "{" + nsmap['gmd'] + "}CI_RoleCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_RoleCode"),
            codeListValue="distributor").text = "distributor"
    md_digitaltransferoptions = etree.SubElement(
            etree.SubElement(
                    md_distribution, "{" + nsmap['gmd'] + "}transferOptions"),
            "{" + nsmap['gmd'] + "}MD_DigitalTransferOptions")
    if size is not None:
        etree.SubElement(
                etree.SubElement(
                        md_digitaltransferoptions,
                        "{" + nsmap['gmd'] + "}transferSize"),
                "{" + nsmap['gco'] + "}Real").text = size
    ci_onlineresource = etree.SubElement(
            etree.SubElement(
                    md_digitaltransferoptions,
                    "{" + nsmap['gmd'] + "}online"),
            "{" + nsmap['gmd'] + "}CI_OnlineResource")
    durl = os.path.join("https://", settings.ARCHIVE['domain'],
                        settings.ARCHIVE['datasets_path'])
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}linkage"),
            "{" + nsmap['gmd'] + "}URL").text = ("/".join([durl, dsid, ""]))
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource,
                    "{" + nsmap['gmd'] + "}protocol"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            durl[0:durl.find("://")])
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            "Dataset Description")
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}description"),
            "{" + nsmap['gco'] + "}CharacterString").text = "Related Link"
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}function"),
            "{" + nsmap['gmd'] + "}CI_OnLineFunctionCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_OnLineFunctionCode"),
            codeListValue="information").text = "information"
    ci_onlineresource = etree.SubElement(
            etree.SubElement(
                    md_digitaltransferoptions,
                    "{" + nsmap['gmd'] + "}onlineResource"),
            "{" + nsmap['gmd'] + "}CI_OnlineResource")
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}linkage"),
            "{" + nsmap['gmd'] + "}URL").text = ("/".join([
                    durl, dsid, "dataaccess", ""]))
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource,
                    "{" + nsmap['gmd'] + "}protocol"),
            "{" + nsmap['gco'] + "}CharacterString").text = (
            durl[0:durl.find("://")])
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}name"),
            "{" + nsmap['gco'] + "}CharacterString").text = "Data Access"
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}description"),
            "{" + nsmap['gco'] + "}CharacterString").text = "Related Link"
    etree.SubElement(
            etree.SubElement(
                    ci_onlineresource, "{" + nsmap['gmd'] + "}function"),
            "{" + nsmap['gmd'] + "}CI_OnLineFunctionCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#CI_OnLineFunctionCode"),
            codeListValue="download").text = "download"


def add_metadata_info(root, nsmap):
    etree.SubElement(
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(root,
                                             ("{" + nsmap['gmd'] +
                                              "}metadataConstraints")),
                            "{" + nsmap['gmd'] + "}MD_Constraints"),
                    "{" + nsmap['gmd'] + "}useLimitation"),
            "{" + nsmap['gco'] + "}CharacterString").text = "none"
    etree.SubElement(
            etree.SubElement(
                    etree.SubElement(
                            etree.SubElement(root,
                                             ("{" + nsmap['gmd'] +
                                              "}metadataMaintenance")),
                            ("{" + nsmap['gmd'] +
                             "}MD_MaintenanceInformation")),
                    "{" + nsmap['gmd'] + "}maintenanceAndUpdateFrequency"),
            "{" + nsmap['gmd'] + "}MD_MaintenanceFrequencyCode",
            codeList=("http://www.isotc211.org/2005/resources/Codelist/"
                      "gmxCodelists.xml#MD_MaintenanceFrequencyCode"),
            codeListValue="asNeeded").text = "asNeeded"


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
        nsmap = {
            'gmd': "http://www.isotc211.org/2005/gmd",
            'gco': "http://www.isotc211.org/2005/gco",
            'gml': "http://www.opengis.net/gml",
            'gmx': "http://www.isotc211.org/2005/gmx",
            'xsi': "http://www.w3.org/2001/XMLSchema-instance",
            'xlink': "http://www.w3.org/1999/xlink",
        }
        schema_loc = etree.QName(
                nsmap['xsi'],
                "schemaLocation")
        nil_reason = etree.QName(
                nsmap['gco'],
                "nilReason")
        root = etree.Element(
                "{" + nsmap['gmd'] + "}MD_Metadata",
                {schema_loc: " ".join([
                        nsmap['gmd'], nsmap['gmd'] + "/gmd.xsd",
                        nsmap['gmx'], nsmap['gmx'] + "/gmx.xsd"])},
                nsmap=nsmap)
        xml_root = open_dataset_overview(dsid)
        add_file_identifier(root, nsmap, dsid)
        add_language(root, nsmap)
        add_charset(root, nsmap)
        add_hierarchy_level(root, nsmap)
        add_contact(root, nsmap)
        add_date_stamp(root, nsmap,
                       metadata_date(dsid, tstamp, mcursor).strftime(
                               "%Y-%m-%dT%H:%M:%SZ"))
        add_metadata_standard(root, nsmap)
        add_dataset_uri(root, nsmap, dsid, mcursor)
        lst = xml_root.findall("./relatedResource")
        if len(lst) > 0:
            add_related_resources(root, nsmap, lst)

        add_data_identification(root, nsmap, nil_reason, dsid, mcursor,
                                wcursor, xml_root)
        size = get_dataset_size(dsid, mcursor, valueOnly="Mbytes")
        add_distribution_info(root, nsmap, dsid, size)
        add_metadata_info(root, nsmap)
    finally:
        mconn.close()
        wconn.close()

    return etree.tostring(root, pretty_print=True).decode("utf-8")
