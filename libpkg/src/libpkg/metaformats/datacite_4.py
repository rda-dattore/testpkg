import json
import psycopg2

from lxml import etree

from . import settings
from ..dbutils import uncompress_bitmap_values
from ..gridutils import spatial_domain_from_grid_definition
from ..metautils import (get_dataset_size,
                         get_date_from_precision,
                         get_pages,
                         open_dataset_overview)
from ..strutils import snake_to_capital
from ..xmlutils import convert_html_to_text


def get_mandatory_fields(dsid, xml_root, cursor):
    mand = {
        'titles': [
                {'title': xml_root.find("./title").text}
        ],
        'types': {
            'resourceTypeGeneral': "Dataset",
            'resourceType':
                xml_root.find("./topic[@vocabulary='ISO']").text
        },
        'publisher': {
        },
    }
    if (mand['titles'][0]['title'][0:26].lower() ==
            "icarus chamber experiment:"):
        pkey = 'icarus'
    else:
        pkey = 'default'

    mand['publisher']['name'] = settings.ARCHIVE['pub_name'][pkey]['name']
    if 'ror' in settings.ARCHIVE['pub_name'][pkey]:
        mand['publisher']['schemeUri'] = "https://ror.org/"
        mand['publisher']['publisherIdentifier'] = (
                settings.ARCHIVE['pub_name'][pkey]['ror'])
        mand['publisher']['publisherIdentiferScheme'] = "ROR"

    mand['creators'] = []
    lst = xml_root.findall("./author")
    if len(lst) > 0:
        for author in lst:
            mand['creators'].append({})
            type = author.get("xsi:type")
            if type is None or type == "authorPerson":
                mand['creators'][-1].update({
                    'name': ", ".join([author.get("lname"),
                                       author.get("fname")]),
                    'nameType': "Personal",
                    'givenName': author.get("fname"),
                    'familyName': author.get("lname"),
                })
                orcid_id = author.get("orcid_id")
                if orcid_id is not None:
                    mand['creators'][-1].update({
                       'nameIdentifiers': [{
                           'schemeUri': "https://orcid.org",
                           'nameIdentifier': "https://orcid.org/" + orcid_id,
                           'nameIdentifierScheme': "ORCID"}]})

            else:
                mand['creators'][-1].update({
                    'name': author.get("name").replace("&", "&amp;"),
                    'nameType': "Organizational"
                })

    if len(mand['creators']) == 0:
        cursor.execute(("select g.path, c.contact from search."
                        "contributors_new as c left join search."
                        "gcmd_providers as g on g.uuid = c.keyword where c."
                        "dsid = %s and c.vocabulary = 'GCMD'"), (dsid, ))
        res = cursor.fetchall()
        for e in res:
            parts = e[0].split(" > ")
            if parts[-1] == "UNAFFILIATED INDIVIDUAL":
                nparts = e[1].split(",")
                lname = (nparts[-1][0].upper() +
                         nparts[1:].lower()).replace(" ", "_")
                mand['creators'].append({
                    'name': ", ".join([lname, nparts[0]]),
                    'nameType': "Personal",
                    'givenName': nparts[0],
                    'familyName': lname,
                })
            else:
                mand['creators'].append({
                    'name': parts[-1]
                                .replace(", ", "/")
                                .replace("&", "&amp;"),
                    'nameType': "Organizational"})

    if len(mand['creators']) == 0:
        raise RuntimeError(("no creators found and this is a required "
                            "DataCite field"))

    cursor.execute("select pub_date from search.datasets where dsid = %s",
                   (dsid, ))
    res = cursor.fetchall()
    if len(res) != 1:
        raise RuntimeError("missing or invalid row count for publication date")

    mand['publicationYear'] = str(res[0][0])[0:4]
    return mand


def to_json(dc_data):
    return (json.dumps(dc_data, indent=2), "")


def to_xml(dc_data, **kwargs):
    warnings = []
    nsmap = {
        None: "http://datacite.org/schema/kernel-4",
        'xsi': "http://www.w3.org/2001/XMLSchema-instance"
    }
    attr_qname = etree.QName(
            "http://www.w3.org/2001/XMLSchema-instance",
            "schemaLocation")
    xsd = "http://schema.datacite.org/meta/kernel-4.4/metadata.xsd"
    root = etree.Element(
            "resource",
            {attr_qname: (
                " ".join([nsmap[None], xsd]))},
            nsmap=nsmap)
    identifier = etree.SubElement(root, "identifier", identifierType="DOI")
    if 'doi' in dc_data:
        identifier.text = dc_data['doi']
    else:
        identifier.text = "N/A"

    if len(dc_data['creators']) > 0:
        creators = etree.SubElement(root, "creators")
        for creator in dc_data['creators']:
            e = etree.SubElement(creators, "creator")
            etree.SubElement(e, "creatorName",
                             nameType=creator['nameType']).text = (
                    creator['name'])
            if creator['nameType'] == "Personal":
                etree.SubElement(e, "givenName").text = creator['givenName']
                etree.SubElement(e, "familyName").text = creator['familyName']
                if 'nameIdentifiers' in creator:
                    nid = creator['nameIdentifiers'][0]
                    etree.SubElement(
                            e, "nameIdentifier",
                            nameIdentifierScheme=nid['nameIdentifierScheme'],
                            schemeURI=nid['schemeUri']).text = (
                                    nid['nameIdentifier'])

    etree.SubElement(
            etree.SubElement(root, "titles"),
            "title").text = dc_data['titles'][0]['title']
    sub_e = etree.SubElement(root, "publisher")
    sub_e.text = dc_data['publisher']['name']
    if ('publisherIdentifierScheme' in dc_data['publisher'] and
            dc_data['publisher']['publisherIdentifierScheme'] == "ROR"):
        sub_e.set("publisherIdentifier",
                  dc_data['publisher']['publisherIdentifier'])
        sub_e.set("publisherIdentifierScheme", "ROR")
        sub_e.set("schemeURI", "https://ror.org/")

    etree.SubElement(root, "publicationYear").text = (
            dc_data['publicationYear'])
    etree.SubElement(
            root, "resourceType",
            resourceTypeGeneral=dc_data['types'][
                                'resourceTypeGeneral']).text = (
            dc_data['types']['resourceType'])
    if 'mandatoryOnly' in kwargs and kwargs['mandatoryOnly']:
        return etree.tostring(root, pretty_print=True).decode("utf-8")

    if len(dc_data['subjects']) > 0:
        subjects = etree.SubElement(root, "subjects")
        for subject in dc_data['subjects']:
            e = etree.SubElement(
                    subjects, "subject",
                    subjectScheme=subject['subjectScheme'],
                    schemeURI=subject['schemeUri'],
                    valueURI=subject['valueUri']).text = subject['subject']

    c = dc_data['contributors'][0]
    etree.SubElement(
            etree.SubElement(
                    etree.SubElement(
                            root, "contributors"),
                    "contributor",
                    contributorType=c['contributorType']),
            "contributorName").text = c['name']
    if len(dc_data['dates']) > 0:
        d = dc_data['dates'][0]
        etree.SubElement(
                etree.SubElement(root, "dates"),
                "date",
                dateType=d['dateType']).text = d['date']

    d = dc_data['descriptions'][0]
    etree.SubElement(
            etree.SubElement(root, "descriptions"),
            "description", descriptionType=d['descriptionType']).text = (
            d['description'])
    if len(dc_data['geoLocations']) > 0:
        geolocs = etree.SubElement(root, "geoLocations")
        for loc in dc_data['geoLocations']:
            e = etree.SubElement(geolocs, "geoLocation")
            keys = list(loc.keys())
            if keys[0] == "geoLocationBox":
                box = etree.SubElement(e, "geoLocationBox")
                etree.SubElement(box, "westBoundLongitude").text = (
                        loc['geoLocationBox']['westBoundLongitude'])
                etree.SubElement(box, "eastBoundLongitude").text = (
                        loc['geoLocationBox']['eastBoundLongitude'])
                etree.SubElement(box, "southBoundLatitude").text = (
                        loc['geoLocationBox']['southBoundLatitude'])
                etree.SubElement(box, "northBoundLatitude").text = (
                        loc['geoLocationBox']['northBoundLatitude'])
            elif keys[0] == "geoLocationPlace":
                etree.SubElement(e, "geoLocationPlace").text = (
                        loc['geoLocationPlace'])

    etree.SubElement(root, "language").text = dc_data['language']
    alt_ids = etree.SubElement(root, "alternateIdentifiers")
    for alt_id in dc_data['alternateIdentifiers']:
        e = etree.SubElement(
                alt_ids, "alternateIdentifier",
                alternateIdentifierType=alt_id['identifierType']).text = (
            alt_id['identifier'])

    if 'relatedItems' in dc_data:
        rel_items = etree.SubElement(root, "relatedItems")
        for rel_item in dc_data['relatedItems']:
            e = etree.SubElement(
                    rel_items, "relatedItem",
                    relatedItemType=rel_item['relatedItemType'],
                    relationType=rel_item['relationType'])
            if 'relatedItemIdentifier' in rel_item:
                sub_e = etree.SubElement(e, "relatedItemIdentifier")
                sub_e.set("relatedItemIdentifierType",
                          rel_item['relatedItemIdentifierType'])
                sub_e.text = rel_item['relatedItemIdentifier']

            etree.SubElement(
                    etree.SubElement(e, "titles"),
                    "title").text = rel_item['titles'][0]['title']
            etree.SubElement(e, "publicationYear").text = (
                    rel_item['publicationYear'])
            if 'issue' in rel_item:
                etree.SubElement(e, "issue").text = rel_item['issue']

            if 'number' in rel_item:
                etree.SubElement(e, "number").text = rel_item['number']

            if 'firstPage' in rel_item:
                etree.SubElement(e, "firstPage").text = rel_item['firstPage']
                etree.SubElement(e, "lastPage").text = rel_item['lastPage']

            if 'publisher' in rel_item:
                etree.SubElement(e, "publisher").text = rel_item['publisher']

    if len(dc_data['relatedIdentifiers']) > 0:
        rel_ids = etree.SubElement(root, "relatedIdentifiers")
        for rel_id in dc_data['relatedIdentifiers']:
            e = etree.SubElement(
                    rel_ids, "relatedIdentifier",
                    relatedIdentifierType=rel_id['relatedIdentifierType'],
                    relationType=rel_id['relationType'])
            e.text = rel_id['relatedIdentifier']
            if 'resourceTypeGeneral' in rel_id:
                e.set("resourceTypeGeneral", rel_id['resourceTypeGeneral'])

    etree.SubElement(etree.SubElement(root, "sizes"), "size").text = (
            dc_data['sizes'][0])
    if len(dc_data['formats']) > 0:
        fmts = etree.SubElement(root, "formats")
        for fmt in dc_data['formats']:
            etree.SubElement(fmts, "format").text = fmt

    rights = dc_data['rightsList'][0]
    etree.SubElement(
            etree.SubElement(root, "rightsList"),
            "rights", rightsIdentifier=rights['rightsIdentifier'],
            rightsURI=rights['rightsUri']).text = rights['rights']
    xml_schema = etree.XMLSchema(etree.parse(xsd))
    root = etree.fromstring(etree.tostring(root))
    try:
        xml_schema.assertValid(root)
    except Exception as err:
        warnings.append("XML validation failed: '" + str(err) + "'")

    return (etree.tostring(root, pretty_print=True).decode("utf-8"),
            "\n".join(warnings))


def to_output(dc_data, ofmt, **kwargs):
    if ofmt == "json":
        return to_json(dc_data)
    else:
        return to_xml(dc_data, **kwargs)


def export(dsid, metadb_settings, wagtaildb_settings, **kwargs):
    try:
        metadb_conn = psycopg2.connect(**metadb_settings)
        metadb_cursor = metadb_conn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    try:
        dc_data = {
            'alternateIdentifiers': [
                {'identifier': ("https://" + settings.ARCHIVE['domain'] + "/" +
                                settings.ARCHIVE['datasets_path'] + "/" +
                                dsid),
                 'identifierType': "URL"},
                {'identifier': dsid,
                 'identifierType': "Local"}],
            'contributors': [
                {'name':
                    ("University Corporation For Atmospheric Research (UCAR):"
                     "NSF National Center for Atmospheric Research (NSF NCAR):"
                     "Computational and Information Systems Laboratory (CISL):"
                     "Information Services Division (ISD):Data Engineering "
                     "and Curation Section (DECS)"),
                 'contributorType': "HostingInstitution"}],
            'language': "en-US"}
        xml_root = open_dataset_overview(dsid)
        resourceTypeGeneral_xml = {
            'book': "Book",
            'book_chapter': "BookChapter",
            'journal:IsDescribedBy': "DataPaper",
            'journal': "JournalArticle",
            'preprint': "ConferenceProceeding",
            'technical_report': "Report",
        }
        resourceTypeGeneral_db = {
            'C': "BookChapter",
            'J': "JournalArticle",
            'P': "ConferenceProceeding",
        }
        ofmt = kwargs['fmt'] if 'fmt' in kwargs else 'xml'
        warnings = []
        metadb_cursor.execute(("select doi from dssdb.dsvrsn where dsid = %s "
                               "and status = 'A' and end_date is null"),
                              (dsid, ))
        res = metadb_cursor.fetchall()
        if len(res) == 1:
            dc_data['doi'] = res[0][0]

        dc_data.update(get_mandatory_fields(dsid, xml_root, metadb_cursor))
        if 'mandatoryOnly' in kwargs and kwargs['mandatoryOnly']:
            o, warn = to_output(dc_data, ofmt, kwargs)
            if len(warn) > 0:
                warnings.append(warn)

            return (o, "\n".join(warnings))

        geocover = xml_root.find("./contentMetadata/geospatialCoverage")
        metadb_cursor.execute((
                "select g.path, g.uuid from search.variables as v left join "
                "search.gcmd_sciencekeywords as g on g.uuid = v.keyword where "
                "v.dsid = %s and v.vocabulary = 'GCMD'"), (dsid, ))
        res = metadb_cursor.fetchall()
        dc_data['subjects'] = []
        for e in res:
            dc_data['subjects'].append({
                'subject': e[0],
                'valueUri': ("https://gcmd.earthdata.nasa.gov/kms/concept/" +
                             e[1]),
                'schemeUri': "https://gcmd.earthdata.nasa.gov/kms",
                'subjectScheme': "GCMD"})

        dc_data['dates'] = []
        metadb_cursor.execute((
                "select min(p.date_start), min(p.start_flag), max(p."
                "date_end), min(p.end_flag) from (select cast(date_start as "
                "text), start_flag, cast(date_end as text), end_flag from "
                "dssdb.dsperiod where dsid = %s and time_zone = 'BCE') as p "
                "having min(p.date_start) is not null and max(p.date_end) is "
                "not null"), (dsid, ))
        res = metadb_cursor.fetchone()
        if res is not None:
            dc_data['dates'].append(
                {'date': (get_date_from_precision(res[0], res[1], 'BCE') +
                          " to " + get_date_from_precision(res[2], res[3],
                                                           'BCE')),
                 'dateType': "Valid"})

        metadb_cursor.execute((
                "select min(p.start), min(p.start_flag), max(p.end), max(p."
                "end_flag), min(p.time_zone) from (select concat(date_start, "
                "' ', time_start) as start, start_flag, concat(date_end, ' ', "
                "time_end) as end, end_flag, time_zone from dssdb.dsperiod "
                "where dsid = %s and date_start between '0001-01-01' and "
                "'3000-01-01' and date_end between '0001-01-01' and "
                "'3000-01-01' and time_zone != 'BCE') as p having min(p."
                "start) is not null and max(p.end) is not null"), (dsid, ))
        res = metadb_cursor.fetchone()
        if res is not None:
            tz = res[4]
            idx = tz.find(",")
            if idx > 0:
                tz = tz[0:idx]

            dc_data['dates'].append(
                {'date': (get_date_from_precision(res[0], res[1], tz) + " to "
                          + get_date_from_precision(res[2], res[3], tz)),
                 'dateType': "Valid"})

        metadb_cursor.execute((
                "select c.doi_work, w.type, count(a.last_name) from citation."
                "data_citations as c left join (select distinct doi from "
                "dssdb.dsvrsn where dsid = %s) as v on v.doi = c.doi_data "
                "left join citation.works_authors as a on a.id = c.doi_work "
                "left join citation.works as w on w.doi = c.doi_work where v."
                "doi is not null and w.type is not null group by c.doi_work, "
                "w.type having count(a.last_name) > 0"), (dsid, ))
        res = metadb_cursor.fetchall()
        dc_data['relatedIdentifiers'] = []
        for e in res:
            dc_data['relatedIdentifiers'].append({
                'relationType': "IsCitedBy",
                'resourceTypeGeneral': resourceTypeGeneral_db[e[1]],
                'relatedIdentifier': e[0],
                'relatedIdentifierType': "DOI"})

        rel_dois = xml_root.findall("./relatedDOI")
        for e in rel_dois:
            rel = e.get("relationType")
            dc_data['relatedIdentifiers'].append({
                'relationType': rel,
                'relatedIdentifier': e.text,
                'relatedIdentifierType': "DOI"})

        if geocover is None:
            dc_data['geoLocations'] = []
            metadb_cursor.execute((
                    "select tablename from pg_tables where schemaname = %s "
                    "and tablename = %s"), ("WGrML", dsid + "_agrids2"))
            metadb_cursor.fetchall()
            if metadb_cursor.rowcount > 0:
                metadb_cursor.execute((
                        "select distinct grid_definition_codes from "
                        "\"WGrML\"." + dsid + "_agrids2"))
                res = metadb_cursor.fetchall()
                min_wlon, min_slat, max_elon, max_nlat = None, None, None, None
                for e in res:
                    bvals = uncompress_bitmap_values(e[0])
                    for val in bvals:
                        metadb_cursor.execute((
                                "select definition, def_params from \"WGrML\"."
                                "grid_definitions where code = %s"),
                                (str(val), ))
                        gdef = metadb_cursor.fetchone()
                        domain = spatial_domain_from_grid_definition(
                                gdef, centerOn="primeMeridian")
                        if all(domain):
                            min_wlon = (domain['wlon'] if min_wlon is None else
                                        min(domain['wlon'], min_wlon))
                            min_slat = (domain['slat'] if min_slat is None else
                                        min(domain['slat'], min_slat))
                            max_elon = (domain['elon'] if max_elon is None else
                                        max(domain['elon'], max_elon))
                            max_nlat = (domain['nlat'] if max_nlat is None else
                                        max(domain['nlat'], max_nlat))

                if min_wlon is not None:
                    dc_data['geoLocations'].append({
                            'geoLocationBox': {
                                'westBoundLongitude': str(min_wlon),
                                'eastBoundLongitude': str(max_elon),
                                'southBoundLatitude': str(min_slat),
                                'northBoundLatitude': str(max_nlat)
                            }
                    })

            metadb_cursor.execute((
                    "select g.path from search.locations_new as l left join "
                    "search.gcmd_locations as g on g.uuid = l.keyword where l."
                    "dsid = %s and l.vocabulary = 'GCMD' order by g.path"),
                    (dsid, ))
            res = metadb_cursor.fetchall()
            for e in res:
                dc_data['geoLocations'].append({'geoLocationPlace': e[0]})

        dc_data['sizes'] = [get_dataset_size(dsid, metadb_cursor)]
        metadb_cursor.execute((
                "select distinct keyword from search.formats where dsid = %s"),
                (dsid, ))
        res = metadb_cursor.fetchall()
        dc_data['formats'] = [snake_to_capital(e[0]) for e in res]
        license_id = xml_root.find("./dataLicense")
        license_id = "CC-BY-4.0" if license_id is None else license_id.text
        try:
            wagtaildb_conn = psycopg2.connect(**wagtaildb_settings)
            wagtaildb_cursor = wagtaildb_conn.cursor()
        except psycopg2.Error as err:
            raise RuntimeError("wagtail database connection error: '{}'"
                               .format(err))

        wagtaildb_cursor.execute((
                "select url, name from wagtail2.home_datalicense where id = "
                "%s"), (license_id, ))
        res = wagtaildb_cursor.fetchone()
        dc_data['rightsList'] = [{
            'rights': res[1],
            'rightsIdentifier': license_id,
            'rightsUri': res[0]}]
        wagtaildb_conn.close()
        abstract = xml_root.find("./summary")
        html = etree.tostring(abstract).decode().replace("&amp;", "&")
        dc_data['descriptions'] = [{
            'description': convert_html_to_text(html),
            'descriptionType': "Abstract"}]
        if geocover is not None:
            lst = geocover.find("./grid")
            for e in lst:
                pass

        dc_data['relatedItems'] = []
        lst = xml_root.findall("./reference")
        for e in lst:
            rel = e.get("ds_relation")
            if rel is None:
                warnings.append((
                        "related reference '{}: {}' was not exported because "
                        "relationType is missing and this is a DataCite "
                        "required field").format(e.find("./authorList").text,
                                                 e.find("./year").text))
            else:
                doi = e.find("./doi")
                type = e.get("type")
                if doi is None:
                    dc_data['relatedItems'].append({
                        'relatedItemType': resourceTypeGeneral_xml[type],
                        'relationType': rel,
                        'titles': [{
                            'title': e.find("./title").text}],
                        'publicationYear': e.find("./year").text})
                    url = e.find("./url")
                    if url is not None:
                        dc_data['relatedItems'][-1].update({
                            'relatedItemIdentifier': url.text,
                            'relatedItemIdentifierType': "URL"})

                    if type == "book":
                        p = e.find("./publisher")
                        dc_data['relatedItems'][-1]['publisher'] = (
                                p.text + ", " + p.get("place"))
                    elif type == "book_chapter":
                        b = e.find("./book")
                        dc_data['relatedItems'][-1].update({
                            'issue': b.text,
                            'publisher': "Ed." + b.get("editor") + ", " +
                                         b.get("publisher"),
                        })
                        dc_data['relatedItems'][-1].update(
                                get_pages(b.get("pages")))
                    elif type == "journal":
                        p = e.find("periodical")
                        dc_data['relatedItems'][-1].update({
                            'issue': p.text,
                            'number': p.get("number")
                        })
                        dc_data['relatedItems'][-1].update(
                                get_pages(p.get("pages")))
                    elif type == "preprint":
                        c = e.find("./conference")
                        dc_data['relatedItems'][-1].update({
                            'issue': c.text,
                            'publisher': (c.get("host") + ", " +
                                          c.get("location")),
                        })
                        dc_data['relatedItems'][-1].update(
                                get_pages(c.get("pages")))
                    elif type == "technical_report":
                        o = e.find("./organization")
                        dc_data['relatedItems'][-1]['publisher'] = o.text
                        dc_data['relatedItems'][-1].update(
                                get_pages(o.get("pages")))
                        r = o.get("reportID")
                        if r is not None:
                            dc_data['relatedItems'][-1]["number"] = r

                else:
                    if (type + ":" + rel) in resourceTypeGeneral_xml:
                        type += ":" + rel
                    dc_data['relatedIdentifiers'].append({
                        'relationType': rel,
                        'relatedIdentifier': doi.text,
                        'resourceTypeGeneral': resourceTypeGeneral_xml[type],
                        'relatedIdentifierType': "DOI"})

        if len(dc_data['relatedItems']) == 0:
            del dc_data['relatedItems']

        o, warn = to_output(dc_data, ofmt)
        if len(warn) > 0:
            warnings.append(warn)

        return (o, "\n".join(warnings))
    finally:
        metadb_conn.close()
