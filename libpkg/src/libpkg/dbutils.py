def decode_bit(bit, next_val, mult, mult_len):
    if bit == '0':
        # bit '0' is a 'skip' bit
        return (next_val + 1, [])

    bias = mult * mult_len
    if bit == '1':
        # bit '1' is a 'next_val' bit
        return (next_val + 1, [next_val - bias])

    bit = ord(bit)
    if bit in range(ord('A'), ord('Z')):
        # sequence of 2-27 '1' bits
        nvals = bit - 63
        v = []
        for x in range(0, nvals):
            v.append(next_val - bias)
            next_val += 1

        return (next_val, v)

    if bit in range(ord('a'), ord('z')):
        # sequence of 2-27 '0' bits
        return (next_val + bit - 95, [])

    if bit in range(ord('2'), ord('9')):
        # sequence of 1-8 '0' bits, then a '1' bit
        next_val += bit - 49
        return (next_val + 1, [next_val - bias])

    if bit in range(ord('!'), ord(',')):
        # sequence of 9-20 '0' bits, then a '1' bit
        next_val += bit - 24
        return (next_val + 1, [next_val - bias])

    if bit in range(ord(';'), ord('@')):
        # sequence of 21-26 '0' bits, then a '1' bit
        next_val += bit - 38
        return (next_val + 1, [next_val - bias])

    if bit == '.':
        # sequence of 27 '0' bits, then a '1' bit
        next_val += bit - 19
        return (next_val + 1, [next_val - bias])

    raise ValueError("invalid bit '{}' for decode".format(chr(bit)))


def decode_group(bitgrp, next_val, mult, mult_len):
    parts = bitgrp.split("/")
    if len(parts) == 1:
        repeat = 1
    elif len(parts) == 2:
        repeat = int(parts[0])
        del parts[0]
    else:
        raise ValueError("malformed bitmap group '{}'".format(bitgrp))

    vals = []
    for x in range(0, repeat):
        next_val, v = decode_bit(parts[0][0], next_val, mult, mult_len)
        vals.extend(v)

    for bit in parts[0][1:]:
        next_val, v = decode_bit(bit, next_val, mult, mult_len)
        vals.extend(v)

    return (next_val, vals)


def uncompress_bitmap_values(bitmap):
    vals = []
    start = bitmap.find(":")
    if start < 0:
        if bitmap[0] == '!':
            parts = bitmap[1:].split(",")
            vals = [int(part) for part in parts]

        return vals

    parts = bitmap[0:start].split("<N>")
    next_val = int(parts[0])
    mult = int(parts[1]) if len(parts) > 1 else 0
    sects = bitmap.split("-")
    if sects[0][0] != ':':
        sect = sects[0][start+1:]
        for bit in sect:
            next_val, v = decode_bit(bit, next_val, mult, len(vals))
            vals.extend(v)

        del sects[0]

    for sect in sects:
        parts = sect.split("/")
        if len(parts) != 2:
            raise ValueError("malformed bitmap group in bitmap '{}'".format(bitmap))

        if len(parts) == 3:
            parts[1] += "/" + parts[2]

        repeat = int(parts[0])
        if parts[1][0] == '{':
            idx = parts[1].find("}")
            if idx < 0:
                raise ValueError("missing end-of-group delimiter in bitmap '{}'".format(bitmap))

            for x in range(0, repeat):
                next_val, v = decode_group(parts[1][1:idx-1], next_val, mult, len(vals))
                vals.extend(v)

            bsect = parts[1][idx+1:]
        else:
            for x in range(0, repeat):
                next_val, v = decode_bit(parts[1][0], next_val, mult, len(vals))
                vals.extend(v)

            bsect = parts[1][1:]

        for bit in bsect:
            next_val, v = decode_bit(bit, next_val, mult, len(vals))
            vals.extend(v)

    return vals
