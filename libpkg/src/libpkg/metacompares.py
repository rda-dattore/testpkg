def time_range_parts(t):
    tr_parts = [0, "", ""]
    if t.find("(initial+") > 0:
        parts = t.split("+")
        tparts = parts[0].split()
        tr_parts[2] = tparts[-2]
        tr_parts[0] = int(parts[1][0:parts[1].find(" ")])
        tr_parts[1] = parts[2][:-1]
    elif t.find("-") > 0:
        if t.find("Forecast") > 0:
            tr_parts[0] = -1
        else:
            tr_parts[0] = 0

        idx = t.find("-")
        tr_parts[1] = t[0:idx]
        tr_parts[2] = t[idx+1:]
        idx = tr_parts[1].find(" ")
        if idx > 0:
            tr_parts[2] = tr_parts[2][idx+1]

    else:
        tr_parts[0] = 0
        tr_parts[1] = t

    return tr_parts


def compare_time_ranges(t1, t2):
    if t1[0:6] == "Analys":
        return -1

    if t2[0:6] == "Analys":
        return 1

    t1_start, t1_end, t1_label = time_range_parts(t1)
    t2_start, t2_end, t2_label = time_range_parts(t2)
    if len(t1_end) < len(t2_end):
        t1_end = (len(t2_end) - len(t1_end)) * '0' + t1_end

    if len(t2_end) < len(t1_end):
        t2_end = (len(t1_end) - len(t2_end)) * '0' + t2_end

    if t1_end < t2_end:
        return -1

    if t2_end < t1_end:
        return 1

    if t1_start < t2_start:
        return -1

    if t2_start < t1_start:
        return 1

    if t1_label < t2_label:
        return -1

    return 1


def compare_levels(l1, l2):
    l1_parts = l1.split(":")
    l2_parts = l2.split(":")
    if l1_parts[0] == l2_parts[0]:
        vu1_parts = l1_parts[1].split(", ")[0].split()
        vu2_parts = l2_parts[1].split(", ")[0].split()
        if (vu1_parts[1] == "mbar" or vu1_parts[1].find("Pa") >= 0 or
                l1_parts[0][0:5] == "Sigma"):
            if float(vu1_parts[0]) > float(vu2_parts[0]):
                return -1

            return 1

        if float(vu1_parts[0]) < float(vu2_parts[0]):
            return -1

        return 1

    if l1_parts[0] < l2_parts[0]:
        return -1

    return 1
