import os

from lxml import etree


def decode_level(data_format, lmap, ltype, value, mapset):
    mapset_key = ".".join([data_format, lmap, "xml"])
    if mapset_key not in mapset:
        mapset[mapset_key] = etree.parse(os.path.join(
                "/data/web/metadata/LevelTables", mapset_key)).getroot()

    types = ltype.split("-")
    if len(types) == 1:
        tree = mapset[mapset_key].find(f"./level[@code='{ltype}']")
        if tree is None:
            tree = mapset[mapset_key].find(f"./layer[@code='{ltype}']")

        if tree is None:
            return ""

        desc = tree.find("./description")
        if desc is None:
            return ""

        units = tree.find("./units")
        if units is None or units.text is None:
            if value == "0":
                return desc.text

            return "".join([desc.text, ": ", value])

        return "".join([desc.text, ": ", value, " ", units.text])

    tree1 = mapset[mapset_key].find(f"./level[@code='{types[0]}']")
    tree2 = mapset[mapset_key].find(f"./level[@code='{types[1]}']")
    if tree1 is None or tree2 is None:
        return ""

    desc1 = tree1.find("./description")
    desc2 = tree2.find("./description")
    if desc1 is None or desc2 is None:
        return ""

    vals = value.split(",")
    units1 = tree1.find("./units")
    if units1 is not None:
        vals[0] = "".join([vals[0], " ", units1.text])

    units2 = tree2.find("./units")
    if units2 is not None:
        vals[1] = "".join([vals[1], " ", units2.text])

    if desc1.text == desc2.text:
        return "".join(["Layer between two '", desc1.text, "': ", vals[0],
                        ", ", vals[1]])

    return "".join(["Layer between '", desc1.text, "': ", vals[0], " and '",
                    desc2.text, "': ", vals[1]])


def decode_parameter(data_format, parameter_code, mapset):
    pmap, pcode = parameter_code.split(":")
    mapset_key = ".".join([data_format, pmap, "xml"])
    if mapset_key not in mapset:
        mapset[mapset_key] = etree.parse(os.path.join(
                "/data/web/metadata/ParameterTables", mapset_key)).getroot()

    desc = mapset[mapset_key].find(f"./parameter[@code='{pcode}']/description")
    if desc is None:
        return ""

    return desc.text
