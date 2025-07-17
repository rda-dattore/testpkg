import getopt
import html
import inspect
import json
import os
import psycopg2
import re
import requests
import sys

from lxml import etree

from libpkg.dbutils import uncompress_bitmap_values
from libpkg.gridutils import (
        convert_grid_definition,
        spatial_domain_from_grid_definition
)
from libpkg.metaformats import dublin_core, json_ld
from libpkg.metautils import get_date_from_precision, open_dataset_overview

from .utils import name_to_initial, unicode_escape, update_wagtail


def write_meta_and_jsonld(dsid, metadb_config, wagtaildb_config):
    dc_meta = dublin_core.export(
            dsid, metadb_config, wagtaildb_config,
            output="html_meta")
    jsonld = json_ld.export(dsid, metadb_config)
    with open(os.path.join("/data/web/jsonld", dsid + ".jsonld"), "w") as f:
        f.write(dc_meta + "\n")
        f.write('<script type="application/ld+json">\n')
        json.dump(json.loads(jsonld), f, indent=2)
        f.write("\n")
        f.write("</script>")


def get_contributors(dsid, cursor):
    contributors = []
    cursor.execute((
            "select g.path from search.contributors_new as c left join "
            "search.gcmd_providers as g on g.uuid = c.keyword where c.dsid = "
            "%s and c.vocabulary = 'GCMD'"), (dsid, ))
    res = cursor.fetchall()
    for e in res:
        id = e[0]
        idx = id.find(">")
        if idx > 0:
            name = id[idx+1:].strip()
            id = id[0:idx].strip()
        else:
            name = ""

        contributors.append({'id': id, 'name': name})

    return contributors


def get_data_volume(dsid, cursor):
    data_volume = {}
    cursor.execute("select dweb_size from dssdb.dataset where dsid = %s",
                   (dsid, ))
    full_volume, = cursor.fetchone()
    full_volume /= 1000000.
    idx = 0
    units = ["MB", "GB", "TB", "PB"]
    while full_volume > 1000. and idx < (len(units) - 1):
        full_volume /= 1000.
        idx += 1

    data_volume['full'] = " ".join([str(round(full_volume, 2)), units[idx]])
    cursor.execute((
            "select dweb_size, title, grpid from dssdb.dsgroup where dsid  = "
            "%s and pindex = 0 and dweb_size > 0"), (dsid, ))
    res = cursor.fetchall()
    groups = []
    for e in res:
        volume = e[0] / 1000000.
        idx = 0
        while volume > 1000. and idx < (len(units) - 1):
            volume /= 1000.
            idx += 1

        groups.append({})
        groups[-1]['volume'] = " ".join([str(round(volume, 2)), units[idx]])
        groups[-1]['group'] = (
                e[2] if (e[1] is None or len(e[1]) == 0) else e[1])

    if len(groups) > 0:
        data_volume['groups'] = groups

    return data_volume


def add_variable_table(dsid, format, list):
    response = requests.get(
            os.path.join("https://rda.ucar.edu/datasets", dsid, "metadata",
                         format + ".html"))
    appended = False
    if response.status_code == 200:
        list.append({'format': format.upper(),
                     'html': "metadata/" + format + ".html"})
        appended = True

    response = requests.get(
            os.path.join("https://rda.ucar.edu/datasets", dsid, "metadata",
                         format + ".xml"))
    if response.status_code == 200:
        if not appended:
            list.append({'format': format.upper()})

        list[-1].update({'xml': "metadata/" + format + ".xml"})


def get_variables(dsid, cursor):
    variables = {}
    cursor.execute((
            "select split_part(path, ' > ', -1) as var from search.variables "
            "as v left join search.gcmd_sciencekeywords as g on g.uuid = v."
            "keyword where v.vocabulary = 'GCMD' and v.dsid = %s order by "
            "var"), (dsid, ))
    res = cursor.fetchall()
    vars = []
    for e in res:
        vars.append(e[0].title())

    if len(vars) > 0:
        variables['gcmd'] = vars

    tables = []
    add_variable_table(dsid, "grib", tables)
    add_variable_table(dsid, "grib2", tables)
    add_variable_table(dsid, "on84", tables)
    if len(tables) > 0:
        variables['tables'] = tables

    return variables


def get_temporal(dsid, cursor):
    temporal = {}
    cursor.execute((
            "select p.date_start, p.time_start, p.start_flag, p.date_end, "
            "p.time_end, p.end_flag, p.time_zone, g.title, g.grpid from dssdb"
            ".dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid "
            "and p.gindex = g.gindex) where p.dsid = %s and g.pindex = 0 and "
            "date_start > '0001-01-01' and date_start < '5000-01-01' and "
            "date_end > '0001-01-01' and date_end < '5000-01-01' union select "
            "p.date_start, p.time_start, p.start_flag, p.date_end, p."
            "time_end, p.end_flag, p.time_zone, g2.title, NULL from dssdb."
            "dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid "
            "and p.gindex = g.gindex) left join dssdb.dsgroup as g2 on (p."
            "dsid = g2.dsid and g.pindex = g2.gindex) where p.dsid = %s and "
            "date_start > '0001-01-01' and date_start < '5000-01-01' and "
            "date_end > '0001-01-01' and date_end < '5000-01-01' and g2.title "
            "is not null order by title"), (dsid, dsid))
    res = cursor.fetchall()
    if len(res) == 0:
        cursor.execute((
                "select date_start, time_start, start_flag, date_end, "
                "time_end, end_flag, time_zone, NULL, NULL from dssdb."
                "dsperiod where dsid = %s and (time_zone = 'BCE' or ("
                "date_start between '0001-01-01' and '5000-01-01' and "
                "date_end between '0001-01-01' and '5000-01-01'))"), (dsid, ))
        res = cursor.fetchall()

    if len(res) > 0:
        if len(res) > 1:
            cursor.execute((
                    "select distinct gindex from dssdb.dsperiod where dsid = "
                    "%s"), (dsid, ))
            gres = cursor.fetchall()
            if len(gres) > 1:
                cursor.execute((
                        "select min(concat(date_start, ' ', time_start)), min("
                        "start_flag), max(concat(date_end, ' ', time_end)), "
                        "min(end_flag), time_zone from dssdb.dsperiod where "
                        "dsid = %s and date_start > '0001-01-01' and "
                        "date_start < '5000-01-01' and date_end > "
                        "'0001-01-01' and date_end < '5000-01-01' group by "
                        "dsid, time_zone"), (dsid, ))
                start, start_flag, end, end_flag, tz = cursor.fetchone()
                sdt = get_date_from_precision(
                        start, start_flag, tz).replace("T", " ")
                if sdt[-6:].replace(":", "") == tz:
                    sdt = " ".join([sdt[:-6], tz])
                edt = get_date_from_precision(
                        end, end_flag, tz).replace("T", " ")
                if edt[-6:].replace(":", "") == tz:
                    edt = " ".join([edt[:-6], tz])
                temporal['full'] = sdt
                if len(edt) > 0 and edt != sdt:
                    temporal['full'] += " to " + edt

        pds = {}
        for e in res:
            sdt = get_date_from_precision(
                    " ".join([str(e[0]), str(e[1])]), e[2],
                    e[6]).replace("T", " ")
            if sdt[-6:].replace(":", "") == e[6]:
                sdt = " ".join([sdt[:-6], e[6]])

            edt = get_date_from_precision(
                    " ".join([str(e[3]), str(e[4])]), e[5],
                    e[6]).replace("T", " ")
            if edt[-6:].replace(":", "") == e[6]:
                edt = " ".join([edt[:-6], e[6]])

            key = e[7] if (e[7] is not None and len(e[7]) > 0) else e[8]
            if key not in pds:
                pds.update({key: [sdt, edt, e[6]]})
            else:
                if e[6] == "BCE":
                    if sdt > pds[key][0]:
                        pds[key][0] = sdt

                    if edt < pds[key][1]:
                        pds[key][1] = edt

                else:
                    if sdt < pds[key][0]:
                        pds[key][0] = sdt

                    if edt > pds[key][1]:
                        pds[key][1] = edt

        if len(pds) > 1:
            pds = dict(sorted(pds.items()))
            groups = []
            for item in pds.items():
                trng = item[1][0]
                if item[1][2] == "BCE":
                    trng += " BCE"

                if len(item[1][1]) > 0 and item[1][1] != item[1][0]:
                    trng += " to " + item[1][1]

                if item[1][2] == "BCE":
                    trng += " BCE"

                groups.append(trng + " ({})".format(item[0]))

            temporal['groups'] = groups

        else:
            item = list(pds.items())[0]
            trng = item[1][0]
            if item[1][2] == "BCE":
                trng += " BCE"

            trng += " to " + item[1][1]
            if item[1][2] == "BCE":
                trng += " BCE"

            temporal['full'] = trng

    return temporal


def add_author(author, citation, last_author, ignore_orcid_id):
    has_orcid_id = False
    if (not ignore_orcid_id and author[3] is not None and len(author[3]) > 0
            and author[3] != "NULL"):
        has_orcid_id = True

    if len(citation) == 0:
        # first author
        if has_orcid_id:
            citation += (
                    ('<a href="https://orcid.org/{}" target="_orcid">')
                    .format(author[3]))

        citation += "{}, {}".format(
                html.escape(unicode_escape(author[0])),
                name_to_initial(author[1]))
        if len(author[2]) > 0:
            citation += " " + name_to_initial(author[2])

        if has_orcid_id:
            citation += "</a>"

    else:
        # co-author
        citation += ", "
        if last_author:
            citation += "and "

        if has_orcid_id:
            citation += (
                    ('<a href="https://orcid.org/{}" target="_orcid">')
                    .format(author[3]))

        citation += name_to_initial(author[1]) + " "
        if len(author[2]) > 0:
            citation += name_to_initial(author[2]) + " "

        citation += html.escape(unicode_escape(author[0]))
        if has_orcid_id:
            citation += "</a>"

    return citation


def add_book_chapter(doi, cursor, citation):
    cursor.execute((
            "select pages, isbn from citation.book_chapter_works where doi = "
            "%s"), (doi, ))
    book_data = cursor.fetchone()
    if book_data is None:
        return ""

    pages, isbn = book_data
    cursor.execute(
            "select title, publisher from citation.book_works where isbn = %s",
            (isbn, ))
    book_data = cursor.fetchone()
    if book_data is None:
        return ""

    title, publisher = book_data
    citation += "{}. Ed. ".format(html.escape(title))
    cursor.execute((
            "select first_name, middle_name, last_name from citation."
            "works_authors where id = %s and id_type = 'ISBN' order by "
            "sequence"), (isbn, ))
    authors = cursor.fetchall()
    if len(authors) == 0:
        return ""

    auth_s = ""
    for author in authors:
        if len(auth_s) > 0:
            auth_s += ", "

        if author == authors[-1]:
            auth_s += "and "

        auth_s += name_to_initial(author[0]) + " "
        if len(author[1]) > 0:
            auth_s += name_to_initial(author[1]) + " "

        auth_s += html.escape(author[2])

    citation += "{}, {}, {}.".format(auth_s, publisher, pages)
    return citation


def add_journal(doi, cursor, citation):
    cursor.execute((
            "select pub_name, volume, pages from citation.journal_works where "
            "doi = %s"), (doi, ))
    journal_data = cursor.fetchone()
    if journal_data is None:
        return ""

    pub_name, volume, pages = journal_data
    citation += ". <em>{}</em>".format(html.escape(unicode_escape(pub_name)))
    if len(volume) > 0:
        citation += ", <strong>{}</strong>".format(volume)

    if len(pages) > 0:
        citation += ", {}".format(pages)

    citation += (
            (', <a href="https://doi.org/{doi}" target="_doi">'
             'https://doi.org/{doi}</a>').format(doi=doi))

    return citation


def add_proceedings(doi, publisher, cursor, citation):
    cursor.execute((
            "select pub_name, pages from citation.proceedings_works where "
            "doi = %s"), (doi, ))
    pub_data = cursor.fetchone()
    if pub_data is None:
        return ""

    citation += ". <em>{}</em>".format(html.escape(
                                       unicode_escape(pub_data[0])))
    if len(publisher) > 0:
        citation += ", " + publisher

    if len(pub_data[1]) > 0:
        citation += ", " + pub_data[1]

    citation += (
            (', <a href="https://doi.org/{doi} target="_doi">'
             'https://doi.org/{doi}</a>').format(doi=doi))
    return citation


def get_citations(dsid, cursor):
    citations = {}
    cursor.execute((
            "select distinct d.doi_work from citation.data_citations as d "
            "left join dssdb.dsvrsn as v on v.doi = d.doi_data where v.dsid "
            "= %s"), (dsid, ))
    res = cursor.fetchall()
    for e in res:
        doi = e[0]
        cursor.execute((
                "select title, pub_year, type, publisher from citation.works "
                "where doi = %s"), (doi, ))
        pub_data = cursor.fetchone()
        if pub_data is None:
            continue

        title, pub_year, type, publisher = pub_data
        title = title.replace("\\/sub", "/sub")
        title = html.escape(unicode_escape(title))
        if type == "C":
            title = '"' + title + '", in '

        cursor.execute((
                "select last_name, first_name, middle_name, orcid_id from "
                "citation.works_authors where id = %s and id_type = 'DOI' "
                "order by sequence"), (doi, ))
        authors = cursor.fetchall()
        if len(authors) == 0:
            continue

        if pub_year not in citations:
            citations[pub_year] = []

        auth_list = ""
        citation = ""
        for author in authors:
            auth_list = add_author(author, auth_list, (author == authors[-1]),
                                   True)
            citation = add_author(author, citation, (author == authors[-1]),
                                  False)

        citation += ", " + str(pub_year) + ": " + title
        if type == "C":
            citation = add_book_chapter(doi, cursor, citation)
        elif type == "J":
            citation = add_journal(doi, cursor, citation)
        elif type == "P":
            citation = add_proceedings(doi, publisher, cursor, citation)

        if len(citation) > 0:
            citations[pub_year].append((auth_list, citation))

    for key in citations.keys():
        citations[key].sort()

    cit_list = []
    for item in citations.items():
        cit_list.append(item)

    cit_list = sorted(cit_list, reverse=True)
    citations = []
    for item in cit_list:
        citations.append({'year': item[0],
                          'publications': [e[1] for e in item[1]]})

    return citations


def update_wagtail_from_metadata_db(dsid, mcursor, wconn):
    mcursor.execute(
            "select title, summary from search.datasets where dsid = %s",
            (dsid, ))
    title, summary = mcursor.fetchone()
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "dstitle", title, wconn)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "abstract", summary.replace("&amp;", "&"), wconn)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "contributors", json.dumps(get_contributors(dsid, mcursor)),
                   wconn)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "volume", json.dumps(get_data_volume(dsid, mcursor)), wconn)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "variables", json.dumps(get_variables(dsid, mcursor)),
                   wconn)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "temporal", json.dumps(get_temporal(dsid, mcursor)), wconn)
    mcursor.execute((
            "select upper(doi) from dssdb.dsvrsn where dsid = %s and status = "
            "'A'"), (dsid, ))
    doi = mcursor.fetchone()
    if doi is not None:
        update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                       "dsdoi", doi, wconn)
    else:
        update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                       "dsdoi", "", wconn)
    citations = get_citations(dsid, mcursor)
    update_wagtail(dsid, "dataset_citation_datasetcitationpage", "citations",
                   json.dumps(citations), wconn)
    num_citations = 0
    for item in citations:
        num_citations += len(item['publications'])

    update_wagtail(dsid, "dataset_citation_datasetcitationpage",
                   "num_citations", num_citations, wconn)


def add_data_license(dsid, xml, wconn):
    data_license = xml.find("./dataLicense")
    if data_license is not None:
        data_license = data_license.text
    else:
        data_license = "CC-BY-4.0"

    cursor = wconn.cursor()
    cursor.execute((
            "select name, url, img_url from wagtail2.home_datalicense where "
            "id = %s"), (data_license, ))
    data_license = cursor.fetchone()
    if data_license is not None:
        data_license = {'name': data_license[0], 'url': data_license[1],
                        'img_url': data_license[2]}
    else:
        data_license = {}

    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "data_license", json.dumps(data_license), wconn)


def add_related_resources(dsid, xml, wconn):
    ele_list = xml.findall("./relatedResource")
    rsrc_list = []
    for e in ele_list:
        description = e.text.strip()
        if description[-1] == '.' and description[-2] != '.':
            description = description[:-1]

        rsrc_list.append({'description': description, 'url': e.get("url")})

    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "related_rsrc_list", json.dumps(rsrc_list), wconn)


def add_html_field(dsid, xml, element_name, wconn, column_name):
    field = xml.find("./" + element_name)
    if field is not None:
        field = str(etree.tostring(field))
        field = (
                field[field.find("<p>"):field.rfind("</p>")+4].strip()
                .replace("&amp;", "&"))
    else:
        field = ""

    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   column_name, field, wconn)


def add_journal_to_publication(element, pub):
    periodical = element.find("./periodical")
    number = periodical.get("number")
    url = element.find("./url")
    title = element.find("./title").text
    if url is not None:
        pub[-1] += '<a href="{}">{}</a>'.format(url.text, title)
    else:
        pub[-1] += title

    pub[-1] += ". <i>{}</i>, ".format(periodical.text)
    pages = periodical.get("pages")
    if pages == "0-0":
        if number == 0:
            pub[-1] += "Submitted"
        elif number == 1:
            pub[-1] += "Accepted"
        elif number == 2:
            pub[-1] += "In Press"
    else:
        pub[-1] += "<b>{}</b>, ".format(number)
        if pages[0:4] == "AGU:":
            pub[-1] += pages[4:]
        else:
            parts = pages.split("-")
            if len(parts) == 2 and parts[0] == parts[1]:
                pub[-1] += parts[0]
            else:
                pub[-1] += pages

    doi = element.find("./doi")
    if doi is not None:
        pub[-1] += " (DOI: {})".format(doi.text)

    pub[-1] += "."


def add_preprint_to_publication(element, pub):
    conference = element.find("./conference")
    url = element.find("./url")
    title = element.find("./title").text
    if url is not None:
        pub[-1] += '<a href="{}">{}</a>'.format(url.text, title)
    else:
        pub[-1] += title

    pub[-1] += (
            (". <i>Proceedings of the {}</i>, {}, {}")
            .format(conference.text, conference.get("host"),
                    conference.get("location")))
    pages = conference.get("pages")
    if len(pages) > 0:
        pub[-1] += ", " + pages

    doi = element.find("./doi")
    if doi is not None:
        pub[-1] += " (DOI: {})".format(doi.text)

    pub[-1] += "."


def add_technical_report_to_publication(element, pub):
    organization = element.find("./organization")
    url = element.find("./url")
    title = element.find("./title").text
    if url is not None:
        pub[-1] += '<i><a href="{}">{}</a></i>'.format(url.text, title)
    else:
        pub[-1] += "<i>{}</i>".format(title)

    report_id = organization.get("reportID")
    if report_id is not None:
        pub[-1] += " {},".format(report_id)

    pub[-1] += " " + organization.text
    pages = organization.get("pages")
    if len(pages) > 0:
        pub[-1] += ", {} pp.".format(pages)

    doi = element.find("./doi")
    if doi is not None:
        pub[-1] += " (DOI: {})".format(doi.text)

    pub[-1] += "."


def add_book_to_publication(element, pub):
    publisher = element.find("./publisher")
    pub[-1] += (
            ("<i>{}</i>. {}, {}")
            .format(element.find("./title").text, publisher.text,
                    publisher.get("place")))
    doi = element.find("./doi")
    if doi is not None:
        pub[-1] += " (DOI: {})".format(doi.text)

    pub[-1] += "."


def add_book_chapter_to_publication(element, pub):
    book = element.find("./book")
    pub[-1] += (
            ('"{}", in {}. Ed. {}, {}, ')
            .format(element.find("./title").text, book.text,
                    book.get("editor"), book.get("publisher")))
    pages = book.get("pages")
    if pages == "0-0":
        pub[-1] += "In Press"
    else:
        pub[-1] += pages

    doi = element.find("./doi")
    if doi is not None:
        pub[-1] += " (DOI: {})".format(doi.text)

    pub[-1] += "."


def add_publications(dsid, xml, wconn):
    pub_list = xml.findall("./reference")
    pubs = []
    for pub in pub_list:
        year = pub.find("./year").text
        auth_list = pub.find("./authorList").text
        key = year + auth_list
        pubs.append([key, auth_list + ", " + year + ": "])
        ptyp = pub.get("type")
        if ptyp == "journal":
            add_journal_to_publication(pub, pubs[-1])
        elif ptyp == "preprint":
            add_preprint_to_publication(pub, pubs[-1])
        elif ptyp == "technical_report":
            add_technical_report_to_publication(pub, pubs[-1])
        elif ptyp == "book":
            add_book_to_publication(pub, pubs[-1])
        elif ptyp == "book_chapter":
            add_book_chapter_to_publication(pub, pubs[-1])

        ann = pub.find("./annotation")
        if ann is not None:
            pubs[-1][1] += (
                    ('<div class="ms-2 text-muted small">{}</div>')
                    .format(ann.text))

    pubs.sort(reverse=True)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "publications", json.dumps([e[1] for e in pubs]), wconn)


def update_wagtail_from_xml(dsid, xml, wconn):
    dslogo = xml.find("./logo")
    if dslogo is not None:
        dslogo = dslogo.text
    else:
        dslogo = ""

    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "dslogo", dslogo, wconn)
    continuing_update = xml.find("./continuingUpdate")
    if continuing_update.get("value") == "yes":
        update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                       "update_freq",
                       continuing_update.get("frequency").title(), wconn)

    add_html_field(dsid, xml, "acknowledgement", wconn, "acknowledgement")
    add_html_field(dsid, xml, "restrictions/access", wconn, "access_restrict")
    add_html_field(dsid, xml, "restrictions/usage", wconn, "usage_restrict")
    add_data_license(dsid, xml, wconn)
    add_related_resources(dsid, xml, wconn)
    add_publications(dsid, xml, wconn)


def add_related_dslist(dsid, cursor, xml, wconn):
    ele_list = xml.findall("./relatedDataset")
    dslist = []
    for e in ele_list:
        dslist.append(e.get("ID"))

    dslist.sort()
    rel_dsids = []
    old_dsid = re.compile(r"^\d{3}\.\d$")
    for id in dslist:
        if old_dsid.match(id):
            id = "d" + id[0:3] + "00" + id[4]

        cursor.execute((
                "select dsid, title from search.datasets where dsid = %s and "
                "type in ('P', 'H')"), (id, ))
        dsdata = cursor.fetchone()
        if dsdata is not None:
            rel_dsids.append({'dsid': dsdata[0],
                              'title': dsdata[1].replace('"', '\\"')})

    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "related_dslist", json.dumps(rel_dsids), wconn)


def add_format_urls(formats):
    if len(formats) == 0:
        return []

    data_formats = []
    response = requests.get(
            "https://rda.ucar.edu/metadata/FormatReferences.xml")
    if response.status_code == 200:
        froot = etree.fromstring(response.content)
        for format in formats:
            data_formats.append({'description': format.replace("_", " ")})
            e = froot.find("./format[@name='" + format + "']")
            if e is not None:
                url = e.get("href")
                if url is not None:
                    data_formats[-1]['url'] = url

    return data_formats


def add_gridded_coverage(dsid, cursor, wconn):
    cursor.execute((
            'select distinct grid_definition_codes from "WGrML".' + dsid +
            '_agrids2'))
    res = cursor.fetchall()
    gvals = set()
    for bitmap in res:
        gvals.update(uncompress_bitmap_values(bitmap[0]))

    gdefs = []
    min_west = 180.
    max_east = -180.
    min_south = 90.
    max_north = -90.
    for val in gvals:
        cursor.execute((
                'select definition, def_params from "WGrML".grid_definitions '
                'where code = %s'), (val, ))
        res = cursor.fetchone()
        domain = spatial_domain_from_grid_definition(
                res, centerOn="primeMeridian")
        min_west = min(domain['wlon'], min_west)
        max_east = max(domain['elon'], max_east)
        min_south = min(domain['slat'], min_south)
        max_north = max(domain['nlat'], max_north)
        gdefs.append(res)

    scov = {}
    scov['west'] = str(round(abs(min_west), 3))
    if scov['west'][-2:] == ".0":
        scov['west'] = scov['west'][0:-2]

    if min_west < 0.:
        scov['west'] += "W"
    else:
        scov['west'] += "E"

    scov['east'] = str(round(abs(max_east), 3))
    if scov['east'][-2:] == ".0":
        scov['east'] = scov['east'][0:-2]

    if max_east < 0.:
        scov['east'] += "W"
    else:
        scov['east'] += "E"

    scov['south'] = str(round(abs(min_south), 3))
    if scov['south'][-2:] == ".0":
        scov['south'] = scov['south'][0:-2]

    if min_south < 0.:
        scov['south'] += "S"
    else:
        scov['south'] += "N"

    scov['north'] = str(round(abs(max_north), 3))
    if scov['north'][-2:] == ".0":
        scov['north'] = scov['north'][0:-2]

    if max_north < 0.:
        scov['north'] += "S"
    else:
        scov['north'] += "N"

    scov['details'] = []
    for gdef in gdefs:
        scov['details'].append(convert_grid_definition(gdef))

    scov['details'].sort()
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "spatial_coverage", json.dumps(scov), wconn)


def check_for_auto_content_metadata(dsid, mconn, wconn):
    has_auto_cmd = False
    cursor = mconn.cursor()
    cursor.execute("select name, data_type from metautil.cmd_databases")
    dblist = cursor.fetchall()
    data_types = []
    formats = []
    for db in dblist:
        try:
            cursor.execute((
                    'select distinct format from "' + db[0] + '".formats '
                    'as f left join "' + db[0] + '".' + dsid +
                    '_webfiles2 as d on d.format_code = f.code where d.'
                    'format_code is not null'))
            res = cursor.fetchall()
            if len(res) > 0:
                has_auto_cmd = True
                data_types.append(db[1].title())
                for e in res:
                    format = e[0]
                    if format[0:12] == "proprietary_":
                        format = format[12:] + " (see dataset documentation)"

                    formats.append(format)

                if db[1] == "grid":
                    add_gridded_coverage(dsid, cursor, wconn)

        except psycopg2.Error:
            mconn.rollback()

    data_formats = add_format_urls(formats)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "data_formats", json.dumps(data_formats), wconn)
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "data_types", json.dumps(data_types), wconn)
    return has_auto_cmd


def add_data_types(dsid, xml, wconn):
    dlist = xml.findall("./contentMetadata/dataType")
    data_types = []
    for dtype in dlist:
        data_types.append(dtype.text.title())

    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "data_types", json.dumps(data_types), wconn)


def add_data_formats(dsid, xml, wconn):
    flist = xml.findall("./contentMetadata/format")
    data_formats = []
    formats = []
    for e in flist:
        url = e.get("href")
        format = e.text
        if format[0:12] == "proprietary_":
            format = format[12:]
            if url is None:
                format += " (see dataset documentation)"

        if url is None:
            formats.append(format)
        else:
            data_formats.append({'description': format, 'url': url})

    data_formats.extend(add_format_urls(formats))
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "data_formats", json.dumps(data_formats), wconn)


def add_temporal_frequency(dsid, xml, wconn):
    freq_list = xml.findall("./contentMetadata/temporalFrequency")
    freqs = []
    for freq in freq_list:
        type = freq.get("type")
        if type == "regular":
            number = int(freq.get("number"))
            f = "Every "
            if number > 1:
                f += str(number) + " "

            f += freq.get("unit")
            if number > 1:
                f += "s"

            stats = freq.get("statistics")
            if stats is not None:
                f += " ({})".format(stats.title())

        elif type == "irregular":
            f = "various times per " + freq.get("unit")
            stats = freq.get("statistics")
            if stats is not None:
                f += " ({})".format(stats.title())

        elif type == "climatology":
            unit = freq.get("unit")
            if unit == "hour":
                f = "Hourly"
            elif unit == "day":
                f = "Daily"
            elif unit == "week":
                f = "Weekly"
            elif unit == "month":
                f = "Monthly"
            elif unit == "winter":
                f = "Winter Season"
            elif unit == "spring":
                f = "Spring Season"
            elif unit == "summer":
                f = "Summer Season"
            elif unit == "autumn":
                f = "Autumn Season"
            elif unit == "year":
                f = "Yearly"
            elif unit == "30-year":
                f = "30-year (climate normal)"

            f += " Climatology"

        freqs.append(f)

    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "temporal_freq", ", ".join(freqs), wconn)


def add_vertical_levels(dsid, xml, wconn):
    vlist = []
    levels = xml.findall("./contentMetadata/levels/level")
    for level in levels:
        type = level.get("type")
        value = level.get("value")
        units = level.get("units")
        if value == "0":
            key = (
                    -1.e12 + (ord(type[0]) * 1000000 + ord(type[1]) * 1000 +
                              ord(type[2]) / 1000000000.))
            entry = type
        else:
            key = float(value)
            if units == "mbar" or units == "degK":
                key = -key

            entry = "{} {}".format(value, units)
            if entry[0] == '.':
                entry = "0" + entry

            if type.find("height below") > 1:
                entry = "-" + entry

        vlist.append((key, entry))

    layers = xml.findall("./contentMetadata/levels/layer")
    for layer in layers:
        type = layer.get("type")
        top = layer.get("top")
        bottom = layer.get("bottom")
        if top == "0" and bottom == "0":
            key = (
                    -1.e12 + (ord(type[0]) * 1000000 + ord(type[1]) * 1000 +
                              ord(type[2]) / 1000000000.))
            entry = type
        else:
            if top[0] == '.':
                top = "0" + top

            key = float(top)
            entry = top
            if bottom[0] == '.':
                bottom = "0" + bottom

            if top != bottom:
                entry += "-" + bottom

            entry += " " + layer.get("units")

        vlist.append((key, entry))

    vlist.sort()
    update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                   "levels", json.dumps({'list': [e[1] for e in vlist]}),
                   wconn)


def print_usage(util_name, err):
    if len(str(err)) > 0:
        print("Error: {}\n".format(err))

    print((
        ("usage: {} --mdb=<dict> --wdb=<dict> dnnnnnn").format(util_name)
        .format(sys.argv[0][sys.argv[0].rfind("/")+1:]) + "\n"
        "\n"
        "--mdb=<dict>   <dict> is the metadata database configuration "
        "dictionary\n"
        "--wdb=<dict>   <dict> is the wagtail database configuration "
        "dictionary\n"
        "\n"
        "dnnnnnn        dataset ID"
    ))
    sys.exit(1)


def main():
    util_name = inspect.currentframe().f_code.co_name
    try:
        arg_len = len(sys.argv[1:])
        if arg_len == 0 or (arg_len > 0 and sys.argv[1] == "-h"):
            raise getopt.GetoptError("")

        opts, args = getopt.getopt(sys.argv[1:], "", ["mdb=", "wdb="])
    except getopt.GetoptError as err:
        print_usage(util_name, err)

    if len(opts) != 2:
        print_usage(util_name,
                    "missing or invalid option(s) - check the command usage")

    for opt in opts:
        if opt[0] == "--mdb":
            try:
                metadb_config = json.loads(opt[1])
            except Exception:
                print_usage(util_name, "bad metadata database configuration")

        elif opt[0] == "--wdb":
            try:
                wagtaildb_config = json.loads(opt[1])
            except Exception:
                print_usage(util_name, "bad wagtail database configuration")

    dsid = args[0]
    try:
        mconn = psycopg2.connect(**metadb_config)
        cursor = mconn.cursor()
        cursor.execute("select type from search.datasets where dsid = %s",
                       (dsid, ))
        type, = cursor.fetchone()
        if type == "W":
            sys.exit(0)

        wconn = psycopg2.connect(**wagtaildb_config)
        write_meta_and_jsonld(dsid, metadb_config, wagtaildb_config)
        update_wagtail(dsid, "dataset_description_datasetdescriptionpage",
                       "dstype", type, wconn)
        update_wagtail_from_metadata_db(dsid, mconn.cursor(), wconn)
        xml = open_dataset_overview(dsid)
        update_wagtail_from_xml(dsid, xml, wconn)
        add_related_dslist(dsid, mconn.cursor(), xml, wconn)
        has_auto_cmd = check_for_auto_content_metadata(dsid, mconn, wconn)
        if not has_auto_cmd:
            add_data_types(dsid, xml, wconn)
            add_data_formats(dsid, xml, wconn)
            add_temporal_frequency(dsid, xml, wconn)
            add_vertical_levels(dsid, xml, wconn)

    finally:
        try:
            mconn.close()
        except Exception:
            pass

        try:
            wconn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
