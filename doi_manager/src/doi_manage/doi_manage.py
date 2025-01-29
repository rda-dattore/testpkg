import json
import os
import psycopg2
import site
import subprocess
import sys
import time

from . import local_settings as settings

from lxml import etree as ElementTree

from libpkg.dbutils import uncompress_bitmap_values
from libpkg.metautils import export_to_datacite_4
from libpkg.unixutils import make_tempdir, remove_tempdir, sendmail


DEBUG = False


def on_crash(exctype, value, traceback):
    if DEBUG:
        sys.__excepthook__(exctype, value, traceback)
    else:
        print("{}: {}".format(exctype.__name__, value))


sys.excepthook = on_crash


def configure(auth_key, config):
    pkg_dirs = site.getsitepackages()
    if len(pkg_dirs) == 0:
        raise FileNotFoundError("unable to locate site-packages directory")

    with open(config['identifier'], "r") as f:
        lines = f.read().splitlines()

    f.close()
    test_api_config = {
        'user': "",
        'password': "",
        'host': "",
        'doi_prefix': "",
        'caller': "test",
    }    
    operations_api_config = {
        'user': "",
        'password': "",
        'host': "",
        'doi_prefix': "",
        'caller': "operations",
    }    
    managed_prefixes = []
    metadb_config = {
        'user': "",
        'password': "",
        'host': "",
        'dbname': "",
    }
    wagtaildb_config = {
        'user': "",
        'password': "",
        'host': "",
        'dbname': "",
    }
    notifications = {
        'error': [],
    }
    for line in lines:
        if line[0] != '#':
            parts = [x.strip() for x in line.split("=")]
            if parts[0] == "test_api_username":
                test_api_config['user'] = parts[1]
            elif parts[0] == "test_api_password":
                test_api_config['password'] = parts[1]
            elif parts[0] == "test_api_host":
                test_api_config['host'] = parts[1]
            elif parts[0] == "test_api_doi_prefix":
                test_api_config['doi_prefix'] = parts[1]
            elif parts[0] == "operations_api_username":
                operations_api_config['user'] = parts[1]
            elif parts[0] == "operations_api_password":
                operations_api_config['password'] = parts[1]
            elif parts[0] == "operations_api_host":
                operations_api_config['host'] = parts[1]
            elif parts[0] == "operations_api_doi_prefix":
                operations_api_config['doi_prefix'] = parts[1]
            elif parts[0] == "managed_prefix":
                managed_prefixes.append(parts[1])
            elif parts[0] == "metadata_database_username":
                metadb_config['user'] = parts[1]
            elif parts[0] == "metadata_database_password":
                metadb_config['password'] = parts[1]
            elif parts[0] == "metadata_database_host":
                metadb_config['host'] = parts[1]
            elif parts[0] == "metadata_database_dbname":
                metadb_config['dbname'] = parts[1]
            elif parts[0] == "wagtail_database_username":
                wagtaildb_config['user'] = parts[1]
            elif parts[0] == "wagtail_database_password":
                wagtaildb_config['password'] = parts[1]
            elif parts[0] == "wagtail_database_host":
                wagtaildb_config['host'] = parts[1]
            elif parts[0] == "wagtail_database_dbname":
                wagtaildb_config['dbname'] = parts[1]
            elif parts[0] == "info_notification":
                notifications['info'].append(parts[1])
            elif parts[0] == "error_notification":
                notifications['error'].append(parts[1])

    with open(os.path.join(pkg_dirs[0], "doi_manage", "local_settings.py"), "w") as f:
        f.write("authorization_key = \"" + auth_key + "\"\n")
        f.write("\ntest_api_config = " + json.dumps(test_api_config, indent=4) + "\n")
        f.write("\noperations_api_config = " + json.dumps(operations_api_config, indent=4) + "\n")
        f.write("\nmanaged_prefixes = " + json.dumps(managed_prefixes, indent=4) + "\n")
        f.write("\nmetadb_config = " + json.dumps(metadb_config, indent=4) + "\n")
        f.write("\nwagtaildb_config = " + json.dumps(wagtaildb_config, indent=4) + "\n")
        f.write("\nnotifications = " + json.dumps(notifications, indent=4) + "\n")

    f.close()


def do_url_registration(doi, dsid, api_config, tdir, **kwargs):
    regfile = os.path.join(tdir, dsid + ".reg")
    if 'retire' in kwargs and kwargs['retire']:
        url = "https://rda.ucar.edu/doi/{}/".format(doi);
    else:
        url = "https://rda.ucar.edu/datasets/{}/".format(dsid);
    with open(regfile, "w") as f:
        f.write("doi=" + doi + "\n")
        f.write("url=" + url + "\n")

    f.close()
    # register the URL
    proc = subprocess.run("curl -s --user {user}:{password} -H 'Content-type: text/plain;charset=UTF-8' -X PUT --data-binary @{regfile} https://{host}/doi/{doi}".format(**api_config, doi=doi, regfile=regfile), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err = proc.stderr.decode("utf-8")
    if len(err) > 0:
        err = "Error while registering the URL for DOI/dsid: '{}/{}': '{}'".format(doi, config['identifier'], err)
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            err,
            devel=DEBUG
        )
        raise RuntimeError(err)

    out = proc.stdout.decode("utf-8")
    if out != "OK":
        err = "Unexpected response while registering the URL for DOI/dsid: '{}/{}': '{}'".format(doi, dsid, out)
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            err,
            devel=DEBUG
        )
        raise RuntimeError(err)

    # verify the registration
    proc = subprocess.run("curl -s --user {user}:{password} https://{host}/doi/{doi}".format(**api_config, doi=doi), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err = proc.stderr.decode("utf-8")
    if len(err) > 0:
        err = "Error while retrieving the registered URL for DOI/dsid: '{}/{}': '{}'".format(doi, dsid, err)
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            err,
            devel=DEBUG
        )
        raise RuntimeError(err)

    out = proc.stdout.decode("utf-8")
    if out != url:
        err = "Unexpected response while retrieving the registered URL for DOI/dsid: '{}/{}': '{}'".format(doi, dsid, out)
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            err,
            devel=DEBUG
        )
        raise RuntimeError(err)


def create_doi(config):
    if config['api_config']['caller'] == "operations":
        test_config = config.copy()
        test_config['api_config'] = settings.test_api_config
        out, warn = create_doi(test_config)
        if len(warn) > 0:
            raise RuntimeError("failed test run: '{}'".format(warn))

    try:
        conn = psycopg2.connect(**settings.metadb_config)
        cursor = conn.cursor()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'".format(err))

    try:
        cursor.execute("select type from search.datasets where dsid = %s", (config['identifier'], ))
        res = cursor.fetchone()
        conn.close()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database error: '{}'".format(err))
    else:
        if res is None:
            raise RuntimeError("dataset '{}' not found".format(config['identifier']))

        if res[0] not in ("P", "H"):
            raise RuntimeError("a DOI can only be assigned to a dataset typed as 'primary' or 'historical'")

        dc, warn = export_to_datacite_4(config['identifier'], settings.metadb_config, settings.wagtaildb_config)

        # mint the DOI and send the associated metadata
        tdir = make_tempdir("/tmp")
        if len(tdir) == 0:
            raise FileNotFoundError("unable to create a temporary directory")

        dcfile = os.path.join(tdir, config['identifier'] + ".dc4")
        with open(dcfile, "w") as f:
            f.write(dc)

        f.close()
        proc = subprocess.run("curl -s --user {user}:{password} -H 'Content-type: application/xml;charset=UTF-8' -X PUT -d@{dcfile} https://{host}/metadata/{doi_prefix}".format(**config['api_config'], dcfile=dcfile), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        err = proc.stderr.decode("utf-8")
        if len(err) > 0:
            raise RuntimeError("error while creating DOI: '{}'".format(err))

        out = proc.stdout.decode("utf-8")
        parts = out.split()
        if len(parts) != 2 or parts[0] != "OK":
            raise RuntimeError("unexpected response while creating DOI: '{}'".format(out))

        doi = parts[-1][1:-1]
        out = ["Success: " + doi]

        # register the dereferencing URL for the DOI
        time.sleep(5)
        do_url_registration(doi, config['identifier'], config['api_config'], tdir)
        if config['api_config']['caller'] == "operations":
            out.append("View the DOI at https://commons.datacite.org/?query=" + doi)

    finally:
        remove_tempdir(tdir)

    return ("\n".join(out), warn)


def update_doi(config, **kwargs):
    parts = config['identifier'].split("==")
    if len(parts) != 2:
        raise RuntimeError("invalid relation '{}'".format(config['identifier']))

    doi = parts[0]
    dsid = parts[1]
    tdir = make_tempdir("/tmp")
    if len(tdir) == 0:
        raise FileNotFoundError("unable to create a temporary directory")

    try:
        retire = True if 'retire' in kwargs and kwargs['retire'] else False
        dc, warn = export_to_datacite_4(dsid, settings.metadb_config, settings.wagtaildb_config, mandatoryOnly=retire)
        # validate the DataCite XML before sending it
        root = ElementTree.fromstring(dc).find(".")
        schema_parts = root.get("{http://www.w3.org/2001/XMLSchema-instance}schemaLocation").split()
        xml_schema = ElementTree.XMLSchema(ElementTree.parse(schema_parts[-1]))
        xml_schema.assertValid(root)
        dcfile = os.path.join(tdir, dsid + ".dc4")
        with open(dcfile, "w") as f:
            f.write(dc)

        f.close()
        # send the XML to DataCite
        proc = subprocess.run("curl -s --user {user}:{password} -H 'Content-type: application/xml;charset=UTF-8' -X PUT -d@{dcfile} https://{host}/metadata/{doi}".format(**config['api_config'], dcfile=dcfile, doi=doi), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        err = proc.stderr.decode("utf-8")
        if len(err) > 0:
            err = "Error sending metadata for DOI: '{}': '{}'".format(doi, err)
            sendmail(
                settings.notifications['error'],
                "rdadoi@ucar.edu",
                "DataCite transfer error",
                err,
                devel=DEBUG
            )
            raise RuntimeError(err)
        out = proc.stdout.decode("utf-8")
        if out.find("OK") != 0:
            err = "Unexpected response while sending metadata for DOI: '{}': '{}'".format(doi, out)
            sendmail(
                settings.notifications['error'],
                "rdadoi@ucar.edu",
                "DataCite transfer - bad response",
                err,
                devel=DEBUG
            )
            raise RuntimeError(err)

        do_url_registration(doi, dsid, config['api_config'], tdir, retire=retire)

    finally:
        remove_tempdir(tdir)

    return warn


def main():
    if len(sys.argv[1:]) < 3:
        print((
            "usage: {} <authorization_key> [options...] <mode> <identifier>".format(sys.argv[0][sys.argv[0].rfind("/")+1:]) + "\n"
            "\nmode (must be one of the following):\n"
            "    configure <file>    initialize/update configuration from entries in <file>\n"
            "                        file 'local_settings.py' is created\n"
            "    create <dnnnnnn>    mint and register a new DOI for dataset dnnnnnn\n"
            "    update <relation>   update the <DOI==dsid> relationship for an existing DOI\n"
            "    terminate <DOI>     terminate the DOI and update the URL registration to\n"
            "                        point to a 'dead' landing page\n"
            "\noptions:\n"
            "    --debug  show stack trace for an exception\n"
            "    -t       run in test mode\n"
        ))
        sys.exit(1)

    args = sys.argv[1:]
    auth_key = args[0]
    if len(auth_key) == 0:
        raise ValueError("authorization key cannot be empty")

    del args[0]
    identifier = args[-1]
    del args[-1]
    mode = args[-1]
    del args[-1]
    config = {'identifier': identifier}
    if "--debug" in args:
        DEBUG = True

    if mode == "configure":
        configure(auth_key, config)
    else:
        try:
            if "-t" in args:
                config.update({'api_config': settings.test_api_config})
            else:
                config.update({'api_config': settings.operations_api_config})

        except Exception:
            raise RuntimeError("did you forget to configure the tool?")

        if auth_key != settings.authorization_key:
            raise ValueError("bad authorization key")

        if mode == "create":
            out, warn = create_doi(config)
        elif mode == "update":
            warn = update_doi(config, retire=False)
        elif mode == "terminate":
            warn = update_doi(config, retire=True)
        else:
            raise ValueError("invalid mode")
            sys.exit(1)

        if len(warn) > 0:
            print("Warning(s):\n{}".format(warn))

        if 'out' in locals() and len(out) > 0:
            print(out)

    sys.exit(0)


if __name__ == "__main__":
    main()
