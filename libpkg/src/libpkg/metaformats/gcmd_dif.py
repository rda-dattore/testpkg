import psycopg2

from lxml import etree


def export(dsid, metadb_settings):
    try:
        mconn = psycopg2.connect(**metadb_settings)
        mcursor = mconn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    warnings = []
    try:
        nsmap = {
            None: "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/",
            'xsi': "http://www.w3.org/2001/XMLSchema-instance",
        }
        schema_loc = etree.QName(nsmap['xsi'], "schemaLocation")
        xsd = nsmap[None] + "dif_v9.7.1.xsd"
        root = etree.Element("DIF", {schema_loc: " ".join([nsmap[None], xsd])},
                             nsmap=nsmap)
    finally:
        mconn.close()

    return (etree.tostring(root, pretty_print=True).decode("utf-8"),
            "\n".join(warnings))
