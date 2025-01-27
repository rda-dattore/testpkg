import psycopg2

from lxml import etree as ElementTree

from .dbutils import uncompress_bitmap_values
from .gridutils import spatial_domain_from_grid_definition
from .xmlutils import convert_html_to_text


def get_date_from_precision(dt, precision, tz):
    parts = dt.split()
    if precision > 3:
        tparts = parts[1].split(":")
        precision -= 3
        while len(tparts) > precision:
            del tparts[-1]

        return parts[0] + "T" + ":".join(tparts) + " " + tz
    else:
        dparts = parts[0].split("-")
        while len(dparts) > precision:
            del dparts[-1]

        return "-".join(dparts)


def get_primary_size(dsid, cursor):
    try:
        cursor.execute("select primary_size from dssdb.dataset where dsid = %s", (dsid, ))
        res = cursor.fetchone()
        if res is not None:
            units = [
               "bytes",
               "Kbytes",
               "Mbytes",
               "Gbytes",
               "Tbytes",
               "Pbytes",
            ]
            size = int(res[0])
            num_div = 0
            while size > 999.999999:
                size /= 1000.
                num_div += 1

            return str(round(size, 3)) + " " + units[num_div]

    except psycopg2.Error as err:
        raise RuntimeError(err)

    return None


def get_pages(pages):
    pages = pages.split("-")
    if len(pages) == 2:
        return {
            'pages': {
                'first': pages[0].strip(),
                'last': pages[1].strip(),
            }
        }

    return {}


def export_to_datacite(dsid, xml_root, metadb_cursor, wagtaildb_cursor):
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
    warnings = []
    dc = "<resource xmlns=\"http://datacite.org/schema/kernel-4\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.4/metadata.xsd\">\n"
    creators = []
    lst = xml_root.findall("./author")
    if len(lst) > 0:
        for author in lst:
            creators.append({})
            type = author.get("xsi:type")
            if type is None or type == "authorPerson":
                creators[-1].update({
                    'type': "person",
                    'lname': author.get("lname"),
                    'fname': author.get("fname"),
                })
                orcid_id = author.get("orcid_id")
                if orcid_id is not None:
                    creators[-1].update({'orcid_id': orcid_id})

            else:
                creators[-1].update({
                    'type': "organization",
                    'name': author.get("name").replace("&", "&amp;")
                })

    geocover = xml_root.find("./contentMetadata/geospatialCoverage")
    try:
        metadb_cursor.execute("select doi from dssdb.dsvrsn where dsid = %s and status = 'A' and end_date is null", (dsid, ))
        res = metadb_cursor.fetchall()
        if len(res) == 1:
            doi = res[0][0]

        metadb_cursor.execute("select pub_date from search.datasets where dsid = %s", (dsid, ))
        res = metadb_cursor.fetchall()
        if len(res) != 1:
            raise psycopg2.Error("missing or invalid row count for publication date")

        pub_year = str(res[0][0])[0:4]
        if len(creators) == 0:
            metadb_cursor.execute("select g.path, c.contact from search.contributors_new as c left join search.gcmd_providers as g on g.uuid = c.keyword where c.dsid = %s and c.vocabulary = 'GCMD'", (dsid, ))
            res = metadb_cursor.fetchall()
            for e in res:
                parts = e[0].split(" > ")
                if parts[-1] == "UNAFFILIATED INDIVIDUAL":
                    nparts = e[1].split(",")
                    lname = (nparts[-1][0].upper() + nparts[1:].lower()).replace(" ", "_")
                    creators.append({
                        'type': "personal",
                        'lname': lname,
                        'fname': nparts[0],
                    })
                else:
                    creators.append({'type': "organization", 'name': parts[-1].replace(", ", "/").replace("&", "&amp;")})

        metadb_cursor.execute("select g.path, g.uuid from search.variables as v left join search.gcmd_sciencekeywords as g on g.uuid = v.keyword where v.dsid = %s and v.vocabulary = 'GCMD'", (dsid, ))
        res = metadb_cursor.fetchall()
        subjs = []
        for e in res:
            subjs.append({'keyword': e[0], 'concept': e[1]})

        metadb_cursor.execute("select min(concat(date_start, ' ', time_start)), min(start_flag), max(concat(date_end, ' ', time_end)), min(end_flag), min(time_zone) from dssdb.dsperiod where dsid = %s and date_start > '0001-01-01' and date_start < '3000-01-01' and date_end > '0001-01-01' and date_end < '3000-01-01'", (dsid, ))
        res = metadb_cursor.fetchone()
        if res is not None:
            tz = res[4]
            idx = tz.find(",")
            if idx > 0:
                tz = tz[0:idx]

            dates = {
                'start': get_date_from_precision(res[0], res[1], tz),
                'end': get_date_from_precision(res[2], res[3], tz),
            }

        metadb_cursor.execute("select c.doi_work, w.type, count(a.last_name) from citation.data_citations as c left join (select distinct doi from dssdb.dsvrsn where dsid = %s) as v on v.doi = c.doi_data left join citation.works_authors as a on a.id = c.doi_work left join citation.works as w on w.doi = c.doi_work where v.doi is not null group by c.doi_work, w.type having count(a.last_name) > 0", (dsid, ))
        res = metadb_cursor.fetchall()
        rel_ids = []
        for e in res:
            rel_ids.append({'doi': e[0], 'type': resourceTypeGeneral_db[e[1]], 'rel': "IsCitedBy"})

        if geocover is None:
            geolocs = []
            metadb_cursor.execute("select tablename from pg_tables where schemaname = %s and tablename = %s", ("WGrML", dsid + "_agrids2"))
            metadb_cursor.fetchall()
            if metadb_cursor.rowcount > 0:
                metadb_cursor.execute("select distinct grid_definition_codes from \"WGrML\"." + dsid + "_agrids2")
                res = metadb_cursor.fetchall()
                min_wlon = 999.
                min_slat = 999.
                max_elon = -999.
                max_nlat = -999.
                for e in res:
                    bvals = uncompress_bitmap_values(e[0])
                    for val in bvals:
                        metadb_cursor.execute("select definition, def_params from \"WGrML\".grid_definitions where code = %s", (str(val), ))
                        gdef = metadb_cursor.fetchone()
                        wlon, slat, elon, nlat = spatial_domain_from_grid_definition(gdef, centerOn="primeMeridian")
                        min_wlon = min(wlon, min_wlon)
                        min_slat = min(slat, min_slat)
                        max_elon = max(elon, max_elon)
                        max_nlat = max(nlat, max_nlat)

                if min_wlon < 999.:
                    geolocs.append({
                        'box': {
                            'wlon': str(min_wlon),
                            'slat': str(min_slat),
                            'elon': str(max_elon),
                            'nlat': str(max_nlat),
                        }
                    })

            metadb_cursor.execute("select g.path from search.locations_new as l left join search.gcmd_locations as g on g.uuid = l.keyword where l.dsid = %s and l.vocabulary = 'GCMD' order by g.path", (dsid, ))
            res = metadb_cursor.fetchall()
            for e in res:
                geolocs.append({'place': e[0]})

        size = get_primary_size(dsid, metadb_cursor)
        metadb_cursor.execute("select distinct keyword from search.formats where dsid = %s", (dsid, ))
        res = metadb_cursor.fetchall()
        data_formats = [e[0] for e in res]
        license_id = xml_root.find("./dataLicense")
        license_id = "CC-BY-4.0" if license_id is None else license_id.text
        wagtaildb_cursor.execute("select url, name from wagtail.home_datalicense where id = %s", (license_id, ))
        rights = (license_id, ) + wagtaildb_cursor.fetchone()
    except psycopg2.Error as err:
        raise RuntimeError(err)

    if len(creators) == 0:
        raise RuntimeError("no creators found and this is a required DataCite field")

    dc += "    <identifier identifierType=\"DOI\">"
    if 'doi' in locals():
        dc += doi
    dc += "</identifier>\n"
    if len(creators) > 0:
        dc += "    <creators>\n"
        for creator in creators:
            dc += "        <creator>\n"
            if creator['type'] == "person":
                dc += (
                    "            <creatorName nameType=\"Personal\">" + creator['lname'] + ", " + creator['fname'] + "</creatorName>\n"
                    "            <givenName>" + creator['fname'] + "</givenName>\n"
                    "            <familyName>" + creator['lname'] + "</familyName>\n"
                )
                if 'orcid_type' in creator:
                    dc += "            <nameIdentifier nameIdentifierScheme=\"ORCID\" schemURI=\"https://orcid.org/\">" + creator['orcid_id'] + "</nameIdentifier>\n"
            else:
                dc += "            <creatorName nameType=\"Organizational\">" + creator['name'] + "</creatorName>\n"

            dc += "         </creator>\n"

        dc += "    </creators>\n"

    dc += (
        "    <titles>\n"
        "        <title>" + xml_root.find("./title").text + "</title>\n"
        "    </titles>\n"
        "    <publisher>UCAR/NCAR - Research Data Archive</publisher>\n"
        "    <publicationYear>" + pub_year + "</publicationYear>\n"
        "    <resourceType resourceTypeGeneral=\"Dataset\">" + xml_root.find("./topic[@vocabulary='ISO']").text + "</resourceType>\n"
    )
    if len(subjs) > 0:
        dc += "    <subjects>\n"
        for subj in subjs:
            dc += "        <subject subjectScheme=\"GCMD\" schemeURI=\"https://gcmd.earthdata.nasa.gov/kms\" valueURI=\"https://gcmd.earthdata.nasa.gov/kms/concept/" + subj['concept'] + "\">" + subj['keyword'] + "</subject>\n"

        dc += "    </subjects>\n"

    dc += (
        "    <contributors>\n"
        "        <contributor contributorType=\"HostingInstitution\">\n"
        "<contributorName>University Corporation For Atmospheric Research (UCAR):National Center for Atmospheric Research (NCAR):Computational and Information Systems Laboratory (CISL):Information Services Division (ISD):Data Engineering and Curation Section (DECS)</contributorName>\n"
        "        </contributor>\n"
        "    </contributors>\n"
    )
    if 'dates' in locals():
        dc += (
            "    <dates>\n"
            "        <date dateType=\"Valid\">" + dates['start'] + " to " + dates['end'] + "</date>\n"
            "    </dates>\n"
        )

    abstract = xml_root.find("./summary")
    html = ElementTree.tostring(abstract).decode().replace("&amp;", "&")
    dc += (
        "    <descriptions>\n"
        "        <description descriptionType=\"Abstract\">" + convert_html_to_text(html) + "</description>\n"
        "    </descriptions>\n"
    )
    if geocover is not None:
        lst = geocover.find("./grid")
        for e in lst:
            pass

    if len(geolocs) > 0:
        dc += "    <geoLocations>\n"
        for loc in geolocs:
            dc += "        <geoLocation>\n"
            if 'box' in loc:
                dc += (
                    "            <geoLocationBox>\n"
                    "                <westBoundLongitude>" + loc['box']['wlon'] + "</westBoundLongitude>\n"
                    "                <eastBoundLongitude>" + loc['box']['elon'] + "</eastBoundLongitude>\n"
                    "                <southBoundLatitude>" + loc['box']['slat'] + "</southBoundLatitude>\n"
                    "                <northBoundLatitude>" + loc['box']['nlat'] + "</northBoundLatitude>\n"
                    "            </geoLocationBox>\n"
                )
            elif 'place' in loc:
                dc += "            <geoLocationPlace>" + loc['place'] + "</geoLocationPlace>\n"

            dc += "        </geoLocation>\n"
        dc += "    </geoLocations>\n"
    dc += (
        "    <language>en-US</language>\n"
        "    <alternateIdentifiers>\n"
        "        <alternateIdentifier alternateIdentifierType=\"URL\">https://rda.ucar.edu/datasets/" + dsid + "/</alternateIdentifier>\n"
        "        <alternateIdentifier alternateIdentifierType=\"Local\">" + dsid + "</alternateIdentifier>\n"
        "    </alternateIdentifiers>\n"
    )
    rel_items = []
    lst = xml_root.findall("./reference")
    for e in lst:
        rel = e.get("ds_relation")
        if rel is None:
            rel = "IsReviewedBy"
        if rel is None:
            warnings.append("related reference '{}: {}' was not exported because relationType is missing and this is a DataCite required field".format(e.find("./authorList").text, e.find("./year").text))
        else:
            doi = e.find("./doi")
            type = e.get("type")
            if doi is None:
                rel_items.append({'type': resourceTypeGeneral_xml[type], 'rel': rel, 'url': e.find("./url").text, 'title': e.find("./title").text, 'pub_year': e.find("./year").text})
                if type == "book":
                    p = e.find("./publisher")
                    rel_items[-1]['publisher'] = p.text + ", " + p.get("place")
                elif type == "book_chapter":
                    b = e.find("./book")
                    rel_items[-1].update({
                        'issue': b.text,
                        'publisher': "Ed." + b.get("editor") + ", " + b.get("publisher"),
                    })
                    rel_items[-1].update(get_pages(b.get("pages")))
                elif type == "journal":
                    p = e.find("periodical")
                    rel_items[-1].update({
                        'issue': p.text,
                        'number': p.get("number")
                    })
                    rel_items[-1].update(get_pages(p.get("pages")))
                elif type == "preprint":
                    c = e.find("./conference")
                    rel_items[-1].update({
                        'issue': c.text,
                        'publisher': c.get("host") + ", " + c.get("location"),
                    })
                    rel_items[-1].update(get_pages(c.get("pages")))
                elif type == "technical_report":
                    o = e.find("./organization")
                    rel_items[-1]['publisher'] = o.text
                    rel_items[-1].update(get_pages(o.get("pages")))
                    r = o.get("reportID")
                    if r is not None:
                        rel_items[-1]["number"] = r

            else:
                if (type + ":" + rel) in resourceTypeGeneral_xml:
                    type += ":" + rel
                rel_ids.append({'doi': doi.text, 'type': resourceTypeGeneral_xml[type], 'rel': rel})

    if len(rel_items) > 0:
        dc += "    <relatedItems>\n"
        for e in rel_items:
            dc += (
                "        <relatedItem relatedItemType=\"" + e['type'] + "\" relationType=\"" + e['rel'] + "\">\n"
                "            <relatedItemIdentifier relatedItemIdentifierType=\"URL\">" + e['url'] + "</relatedItemIdentifier>\n"
                "            <titles>\n"
                "                <title>" + e['title'] + "</title>\n"
                "            </titles>\n"
                "            <publicationYear>" + e['pub_year'] + "</publicationYear>\n"
            )
            if 'issue' in e:
                dc += "            <issue>" + e['issue'] + "</issue>\n"

            if 'number' in e:
                dc += "            <number>" + e['number'] + "</number>\n"

            if 'pages' in e:
                dc += (
                    "            <firstPage>" + e['pages']['first'] + "</firstPage>\n"
                    "            <lastPage>" + e['pages']['last'] + "</lastPage>\n"
                )

            if 'publisher' in e:
                dc += "            <publisher>" + e['publisher'] + "</publisher>\n"

            dc += "        </relatedItem>\n"

        dc += "    </relatedItems>\n"

    lst = xml_root.findall("./relatedDOI")
    if len(lst) > 0 or len(rel_ids) > 0:
        dc += "    <relatedIdentifiers>\n"
        for e in lst:
            rel = e.get("relationType")
            if rel is None:
                raise NameError("relationType is missing for related DOI '{}', and this is a DataCite required field".format(e.text))

            dc += "        <relatedIdentifier relatedIdentifierType=\"DOI\" relationType=\"" + rel + "\">" + e.text + "</relatedIdentifier>\n"

        for e in rel_ids:
            dc += "        <relatedIdentifier relatedIdentifierType=\"DOI\" relationType=\"" + e['rel'] + "\" resourceTypeGeneral=\"" + e['type'] + "\">" + e['doi'] + "</relatedIdentifier>\n"

        dc += "    </relatedIdentifiers>\n"

    if size is not None:
        dc += (
            "    <sizes>\n"
            "        <size>" + size + "</size>\n"
            "    </sizes>\n"
        )

    if len(data_formats) > 0:
        dc += "    <formats>\n"
        for e in data_formats:
            dc += "        <format>" + e + "</format>\n"

        dc += "    </formats>\n"

    dc += (
        "    <rightsList>\n"
        "        <rights rightsIdentifier=\"" + rights[0] + "\" rightsURI=\"" + rights[1] + "\">" + rights[2] + "</rights>\n"
        "    </rightsList>\n"
    )
    dc += "</resource>"
    #print(dc)
    return (dc, "\n".join(warnings))
