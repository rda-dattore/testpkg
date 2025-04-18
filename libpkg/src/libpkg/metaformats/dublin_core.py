from lxml import etree


def export_html_meta(dsid, metadb_settings):
    pass


def export_oai_dc(dsid, metadb_settings):
    warnings = []
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
    return (etree.tostring(root, pretty_print=True).decode("utf-8"),
            "\n".join(warnings))


def export(dsid, metadb_settings, **kwargs):
    if 'output' in kwargs and kwargs['output'] == "html_meta":
        return export_html_meta(dsid, metadb_settings)

    return export_oai_dc(dsid, metadb_settings)
