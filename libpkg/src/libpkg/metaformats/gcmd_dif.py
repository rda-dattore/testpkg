import os
import psycopg2

from lxml import etree

from . import settings
from ..metautils import (get_dataset_size, get_date_from_precision,
                         open_dataset_overview)
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

    warnings = []
    try:
        publisher_keyword = (
                "UCAR/NCAR/CISL/DECS > Data Engineering and Curation Section, "
                "Computational and Information Systems Laboratory, National "
                "Center for Atmospheric Research, University Corporation for "
                "Atmospheric Research")
        nsmap = {
            None: "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/",
            'xsi': "http://www.w3.org/2001/XMLSchema-instance",
        }
        schema_loc = etree.QName(nsmap['xsi'], "schemaLocation")
        xsd = nsmap[None] + "dif_v9.7.1.xsd"
        root = etree.Element("DIF", {schema_loc: " ".join([nsmap[None], xsd])},
                             nsmap=nsmap)
        xml_root = open_dataset_overview(dsid)
        entry_id = etree.SubElement(root, "Entry_ID")
        mcursor.execute((
                "select doi from dssdb.dsvrsn where dsid = %s and status = "
                "'A'"), (dsid, ))
        doi = mcursor.fetchone()
        if doi is not None:
            entry_id.text = "DOI:" + doi[0]
        else:
            entry_id.text = "edu.ucar.rda:" + dsid

        mcursor.execute((
                "select title, summary, continuing_update from search."
                "datasets where dsid = %s"), (dsid, ))
        title, summary, progress = mcursor.fetchone()
        summary = convert_html_to_text("<summary>" + summary + "</summary>")
        etree.SubElement(root, "Entry_Title").text = title
        ds_citation = etree.SubElement(root, "Data_Set_Citation")
        creators = []
        mcursor.execute((
                "select type, given_name, middle_name, family_name from "
                "search.dataset_authors where dsid = %s"), (dsid, ))
        res = mcursor.fetchall()
        if len(res) > 0:
            for atype, first, middle, last in res:
                if atype == "Person":
                    author = first
                    if len(middle) > 0:
                        author += " " + middle

                    author += " " + last
                    creators.append(author)
                else:
                    creators.append(first)

        else:
            mcursor.execute((
                    "select g.path from search.contributors_new as c left "
                    "join search.gcmd_providers as g on g.uuid = c.keyword "
                    "where c.dsid = %s and c.vocabulary = 'GCMD'"), (dsid, ))
            res = mcursor.fetchall()
            for contributor, in res:
                creators.append[contributor]

        etree.SubElement(ds_citation, "Dataset_Creator").text = (
                ", ".join(creators))
        etree.SubElement(ds_citation, "Dataset_Title").text = title
        etree.SubElement(ds_citation, "Dataset_Publisher").text = (
                publisher_keyword)
        etree.SubElement(ds_citation, "Online_Resource").text = (
                os.path.join(settings.ARCHIVE['datasets_url'], dsid))
        personnel = etree.SubElement(root, "Personnel")
        etree.SubElement(personnel, "Role").text = "Technical Contact"
        etree.SubElement(personnel, "Email").text = "rdahelp@ucar.edu"
        mcursor.execute((
                "select g.path from search.variables as v left join search."
                "gcmd_sciencekeywords as g on g.uuid = v.keyword where v.dsid "
                "= %s and v.vocabulary = 'GCMD'"), (dsid, ))
        res = mcursor.fetchall()
        for e in res:
            parts = e[0].split(" > ")
            parameters = etree.SubElement(root, "Parameters")
            etree.SubElement(parameters, "Category").text = parts[0]
            etree.SubElement(parameters, "Topic").text = parts[1]
            etree.SubElement(parameters, "Term").text = parts[2]
            etree.SubElement(parameters, "Variable_Level_1").text = parts[3]
            if len(parts) > 4:
                etree.SubElement(parameters, "Variable_Level_2").text = (
                        parts[4])
                if len(parts) > 5:
                    etree.SubElement(parameters, "Variable_Level_3").text = (
                            parts[5])
                    if len(parts) > 6:
                        etree.SubElement(
                                parameters, "Detailed_Variable").text = (
                                parts[6])

        mcursor.execute((
                "select keyword from search.topics where dsid = %s and "
                "vocabulary = 'ISO'"), (dsid, ))
        iso_topic, = mcursor.fetchone()
        etree.SubElement(root, "ISO_Topic_Category").text = iso_topic
        mcursor.execute((
                "select g.path from search.platforms_new as p left join "
                "search.gcmd_platforms as g on g.uuid = p.keyword where p."
                "dsid = %s and p.vocabulary = 'GCMD'"), (dsid, ))
        res = mcursor.fetchall()
        for path, in res:
            source = etree.SubElement(root, "Source_Name")
            idx = path.find(" > ")
            if idx > 0:
                etree.SubElement(source, "Short_Name").text = path[0:idx]
                etree.SubElement(source, "Long_Name").text = path[idx+3:]
            else:
                etree.SubElement(source, "Short_Name").text = path

        mcursor.execute((
                "select min(concat(date_start, ' ', time_start)), min("
                "start_flag), max(concat(date_end, ' ', time_end)), min("
                "end_flag), min(time_zone) from dssdb.dsperiod where dsid = "
                "%s and date_start < '9998-01-01' and date_end < "
                "'9998-01-01'"), (dsid, ))
        res = mcursor.fetchone()
        if res is not None:
            tz = res[4]
            idx = tz.find(",")
            if idx > 0:
                tz = tz[0:idx]

            tc = etree.SubElement(root, "Temporal_Coverage")
            etree.SubElement(tc, "Start_Date").text = (
                    get_date_from_precision(res[0], res[1], tz))
            etree.SubElement(tc, "End_Date").text = (
                    get_date_from_precision(res[2], res[3], tz))

        dsprog = etree.SubElement(root, "Data_Set_Progress")
        if progress == "Y":
            dsprog.text = "In Work"
        else:
            dsprog.text = "Complete"



        mcursor.execute((
                "select distinct g.path from (select keyword from search."
                "projects_new where dsid = %(dsid)s and vocabulary = 'GCMD' "
                "union select keyword from search.supported_projects where "
                "dsid = %(dsid)s  and vocabulary = 'GCMD') as p left join "
                "search.gcmd_projects as g on g.uuid = p.keyword"),
                {'dsid': dsid})
        res = mcursor.fetchall()
        for path, in res:
            project = etree.SubElement(root, "Project")
            idx = path.find(" > ")
            if idx > 0:
                etree.SubElement(project, "Short_Name").text = path[0:idx]
                etree.SubElement(project, "Long_Name").text = path[idx+3:]
            else:
                etree.SubElement(project, "Short_Name").text = path

        wcursor.execute((
                "select access_restrict from wagtail."
                "dataset_description_datasetdescriptionpage where dsid = %s"),
                (dsid, ))
        access = wcursor.fetchone()
        if access is not None and len(access[0]) > 0:
            etree.SubElement(root, "Access_Constraints").text = (
                    convert_html_to_text("<access>" + access[0] + "</access>"))

        wcursor.execute((
                "select usage_restrict from wagtail."
                "dataset_description_datasetdescriptionpage where dsid = %s"),
                (dsid, ))
        usage = wcursor.fetchone()
        if usage is not None and len(usage[0]) > 0:
            etree.SubElement(root, "Use_Constraints").text = (
                    convert_html_to_text("<usage>" + usage[0] + "</usage>"))

        etree.SubElement(root, "Data_Set_Language").text = "English"
        center = etree.SubElement(root, "Data_Center")
        name = etree.SubElement(center, "Data_Center_Name")
        idx = publisher_keyword.find(" > ")
        etree.SubElement(name, "Short_Name").text = publisher_keyword[0:idx]
        etree.SubElement(name, "Long_Name").text = publisher_keyword[idx+3:]
        etree.SubElement(center, "Data_Set_ID").text = dsid
        etree.SubElement(
                etree.SubElement(center, "Personnel"), "Email").text = (
                "rdahelp@ucar.edu")
        etree.SubElement(
                etree.SubElement(root, "Distribution"),
                "Distribution_Size").text = get_dataset_size(dsid, mcursor)



        etree.SubElement(root, "Summary").text = summary
        dslst = xml_root.findall("./relatedDataset")
        for ds in dslst:
            etree.SubElement(
                    etree.SubElement(root, "Related_URL"), "URL").text = (
                    os.path.join(settings.ARCHIVE['datasets_url'],
                                 ds.get("ID"), ""))

        rsrclst = xml_root.findall("./relatedResource")
        for rsrc in rsrclst:
            rurl = etree.SubElement(root, "Related_URL")
            etree.SubElement(rurl, "URL").text = rsrc.get("url")
            etree.SubElement(rurl, "Description").text = rsrc.text

        etree.SubElement(
                etree.SubElement(root, "IDN_Node"), "Short_Name").text = (
                "USA/NCAR")
        etree.SubElement(root, "Metadata_Name").text = "CEOS IDN DIF"
        etree.SubElement(root, "Metadata_Version").text = "9.7"
    finally:
        mconn.close()
        wconn.close()

    return (etree.tostring(root, pretty_print=True).decode("utf-8"),
            "\n".join(warnings))
