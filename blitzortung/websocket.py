#function decode(b) {
#    var a,
#        e = {},
#        d = b.split(""),
#        c = d[0],
#        f = c,
#        g = [c],
#        h = 256;
#        o = h;
#    for (b = 1; b < d.length; b++)
#    a = d[b].charCodeAt(0),
#    a = h > a ? d[b] : e[a] ? e[a] : f + c, g.push(a)
#    c = a.charAt(0), e[o] = f + c
#    o++
#    f = a;
#    return g.join("")
#}

def decode(data):
    h = 256
    e = dict()

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




