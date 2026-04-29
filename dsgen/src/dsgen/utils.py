import html

from unidecode import unidecode


def name_to_initial(name):
    parts = name.strip().split()
    initials = []
    for part in parts:
        part = html.escape(part)
        if part[0] == '&':
            initials.append(part[0:part.find(";")])
        else:
            initials.append(part[0])

        initials[-1] += "."

    return " ".join(initials)


def unicode_escape(s):
    u = unidecode(s).replace("\\\\u", "\\u")
    return u.encode("utf-8").decode("unicode-escape")


def update_wagtail(dsid, table, column, insert_value, conn):
    conn.cursor().execute((
        "update wagtail2." + table + " set " + column + " = %s where dsid = "
        "%s"), (insert_value, dsid))
    conn.commit()
