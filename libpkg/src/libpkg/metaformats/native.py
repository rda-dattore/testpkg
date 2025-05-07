import os
import psycopg2

from lxml import etree

from ..metautils import open_dataset_overview


def export(dsid, metadb_settings):
    try:
        conn = psycopg2.connect(**metadb_settings)
        cursor = conn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'"
                           .format(err))

    try:
        xml_root = open_dataset_overview(dsid)
    finally:
        conn.close()

    return etree.tostring(xml_root, pretty_print=True).decode("utf-8")
