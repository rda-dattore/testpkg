import requests

from lxml import etree


def open_dataset_overview(dsid):
    resp = requests.get("https://rda.ucar.edu/datasets/" + dsid +
                        "/metadata/dsOverview.xml")
    if resp.status_code != 200:
        raise RuntimeError(("unable to download dataset overview: status "
                            "code: {}".format(resp.status_code)))

    return etree.fromstring(resp.text)


def get_date_from_precision(dt, precision, tz):
    parts = dt.split()
    if precision > 3:
        tparts = parts[1].split(":")
        precision -= 3
        while len(tparts) > precision:
            del tparts[-1]

        return parts[0] + "T" + ":".join(tparts) + tz[0:3] + ":" + tz[3:]
    else:
        dparts = parts[0].split("-")
        while len(dparts) > precision:
            del dparts[-1]

        return "-".join(dparts)


def get_dataset_size(dsid, cursor):
    cursor.execute("select dweb_size from dssdb.dataset where dsid = %s",
                   (dsid, ))
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

    return None


def get_pages(pages):
    pages = pages.split("-")
    if len(pages) == 2:
        return {
            'firstPage': pages[0].strip(),
            'lastPage': pages[1].strip(),
        }

    return {}
