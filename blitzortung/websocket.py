def decode(data):
    h = 256
    e = {}

    c = data[0]
    f = c
    g = c
    o = h
    for b, char in enumerate(data[1:]):
        a = ord(char)
        a = char if h > a else e.get(a, f + c)
        g += a

        c = a[0]
        e[o] = f + c
        o += 1
        f = a
    return g
