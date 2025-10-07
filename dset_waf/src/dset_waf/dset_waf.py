import json
import os
import psycopg2
import subprocess
import sys

from datetime import datetime, timezone
from libpkg.metaformats import iso_19139
from libpkg.strutils import strand
from lxml import etree


LOCAL_WAF = "/data/dset_waf"
REPO_HEAD = os.path.join(LOCAL_WAF, "git-repos")

GIT_REPOS = [
    "dset-web-accessible-folder-dev/rda",
    "dash-rda-prod/RDA-Datasets",
]


def do_push(args):
    if len(args) < 3:
        print("Error: missing argument(s) for PUSH")
        sys.exit(1)

    wdb_config = json.loads(args[-1])
    del args[-1]
    mdb_config = json.loads(args[-1])
    del args[-1]
    push_list = []
    if len(args) > 1:
        for arg in args:
            if arg[0] == 'd':
                push_list.append(arg)
            else:
                print("Error: invalid dataset ID '{}'".format(arg))
                sys.exit(1)

    elif args[0][0] == 'd':
        push_list.append(args[0])

    try:
        mconn = psycopg2.connect(**mdb_config)
        mcursor = mconn.cursor()
        if len(push_list) == 0:
            if args[0] == "all":
                mcursor.execute((
                        "select dsid from search.datasets where type in "
                        "('P', 'H')"))
            elif args[0] == "queued-only":
                mcursor.execute((
                        "select w.dsid from metautil.dset_waf as w left join "
                        "search.datasets as d on d.dsid = w.dsid where d.type "
                        "in ('P', 'H') and w.uflag = ''"))
            else:
                print("Error: invalid DSID_LIST")
                sys.exit(1)

            res = mcursor.fetchall()
            push_list = [e[0] for e in res]

        if len(push_list) == 0:
            print("No matching datasets found.")
            sys.exit(1)

        uflag = ""
        if args[0] == "queued-only":
            uflag = strand(10)
            mcursor.execute("update metautil.dset_waf set uflag = %s",
                            (uflag, ))
            mconn.commit()

        wconn = psycopg2.connect(**wdb_config)
        wcursor = wconn.cursor()
        xml_schema = etree.XMLSchema(
                etree.parse("/data/dset_waf/schemas/iso/iso19139.xsd"))
        failed_validation_set = set()
        for dsid in push_list:
            iso_rec = iso_19139.export(dsid, mdb_config, wdb_config)
            date_stamp_index = iso_rec.find("<gmd:dateStamp>")
            date_time_index = iso_rec.find("<gco:DateTime>", date_stamp_index)
            current_utc_datetime = (datetime.now(timezone.utc)
                                    .strftime("%Y-%m-%dT%H:%M:%SZ"))
            iso_rec = (iso_rec[0:date_time_index+14] + "" +
                       current_utc_datetime + iso_rec[date_time_index+33:])
            print(iso_rec)
            # validate the ISO record
            root = etree.fromstring(iso_rec).find(".")
            try:
                xml_schema.assertValid(root)
            except Exception as err:
                print("Error: {} failed to validate: '{}'".format(dsid, err))
                failed_validation_set.add(dsid)

        if len(failed_validation_set) > 0:
            push_list = [e for e in push_list if e not in
                         failed_validation_set]

        for repo in GIT_REPOS:
            repo_path = os.path.join(REPO_HEAD, repo)
            o = subprocess.run((
                    "git -C " + repo_path + " stash; git -C " + repo_path +
                    " pull -q"), shell=True, capture_output=True)
            err = o.stderr.decode("utf-8")
            if len(err) > 0:
                if o.returncode == 0:
                    print(("git pull message: '{}'; uflag was '{}'")
                          .format(err, uflag))
                else:
                    print(("git pull error: '{}'; uflag was '{}'")
                          .format(err, uflag))
                    sys.exit(1)

            added = False
            for dsid in push_list:
                waf_name = os.path.join(LOCAL_WAF, "waf-" + dsid + ".xml")

    except Exception as err:
        print("An error occurred: '{}'".format(err))
    finally:
        if 'mconn' in locals():
            mconn.close()

        if 'wconn' in locals():
            wconn.close()


def do_delete(args):
    print("do_delete")


def do_dbreset(args):
    if len(args) < 1:
        print("Error: missing DB configuration")
        sys.exit(1)

    db_config = json.loads(args[0])
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("select distinct dsid from metautil.dset_waf")
        dsids = cursor.fetchall()
        try:
            cursor.execute("delete from metautil.dset_waf")
            conn.commit()
            for dsid in dsids:
                cursor.execute("insert into metautil.dset_waf values (%s, '')",
                               (dsid, ))
                conn.commit()
        except Exception as err:
            print(("Error while trying to clear a failed push: '{}', list: "
                   "'{}'").format(err, ", ".join(dsids)))

    except Exception as err:
        print("Error while trying to fix a failed push: '{}'".format(err))
    finally:
        if 'conn' in locals():
            conn.close()


ACTIONS = {
    'PUSH': {
        'description': "add/update dataset(s)",
        'callable': do_push,
    },
    'DELETE': {
        'description': "remove dataset(s)",
        'callable': do_delete,
    },
    'DBRESET': {
        'description': "reset the database after a failed push",
        'callable': do_dbreset,
    },
}


def print_usage_and_exit():
    print(("usage: dset_waf PUSH DSID_LIST META_DBCONFIG WAGTAIL_CONFIG"))
    print("  or:  dset_waf DELETE DSID_LIST META_DBCONFIG")
    print("  or:  dset_waf DBRESET META_DBCONFIG")
    print("")
    print("valid actions:")
    for key, val in ACTIONS.items():
        print("    " + " - ".join([key, val['description']]))

    print("")
    print("valid DSID_LIST specifications:")
    print(("    'dnnnnnn ...':          specify one or more individual "
           "dataset IDs"))
    print(("    'all':                  identify all public datasets"))
    print(("    'queued-only' (PUSH):   only push datasets that are queued in "
           "the database"))
    print(("    'non-public' (DELETE):  identify and delete just "
           "non-public datasets"))
    print("")
    print(("META_DBCONFIG   dictionary of metadata DB configuration "
           "parameters as:"))
    print(('    {"user": "<U>", "password": "<P>", "host": "<H>", "dbname": '
           '"<D>"}'))
    print(("         where U = username, P = password, H = host name, D = "
           "database name"))
    print("")
    print(("WAGTAIL_CONFIG  dictionary of wagtail DB configuration "
           "parameters (like"))
    print("                META_DBCONFIG)")
    sys.exit(1)


def main():
    if len(sys.argv) == 1 or sys.argv[1] == "--help":
        print_usage_and_exit()

    if sys.argv[1] not in ACTIONS.keys():
        print("Error: invalid action - must be one of: " +
              ", ".join([("'" + key + "'") for key in ACTIONS.keys()]))
        sys.exit(1)

    if 'callable' not in ACTIONS[sys.argv[1]]:
        raise KeyError("no callable found for this action")

    ACTIONS[sys.argv[1]]['callable'](sys.argv[2:])
