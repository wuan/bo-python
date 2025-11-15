import pytest
from mock import patch


example_data = """{"time":1763202124325980200,"lat":-15.296556,"lon":134.589548,"alt":0,"pol":0,"mds":12581,"mcg":162,"status":0,"region":2}
{"time":1763202124297904000,"lat":44.283328,"lon":8.910987,"alt":0,"pol":0,"mds":6830,"mcg":84,"status":2,"region":9}
{"time":1763202124297904000,"lat":44.283328,"lon":8.910987,"alt":0,"pol":0,"mds":6830,"mcg":84,"status":1,"region":8}
{"time":1763202124297897200,"lat":44.288127,"lon":8.927448,"alt":0,"pol":0,"mds":4934,"mcg":136,"status":2,"region":9}
{"time":1763202124297897200,"lat":44.288127,"lon":8.927448,"alt":0,"pol":0,"mds":4934,"mcg":136,"status":1,"region":8}
{"time":1763202124297892000,"lat":44.2774,"lon":8.929396,"alt":0,"pol":0,"mds":8913,"mcg":138,"status":0,"region":1}
{"time":1763202124101646800,"lat":-41.585918,"lon":152.926124,"alt":0,"pol":0,"mds":13391,"mcg":176,"status":0,"region":2}
{"time":1763202123983937500,"lat":44.284989,"lon":8.915263,"alt":0,"pol":0,"mds":10832,"mcg":178,"status":2,"region":9}
{"time":1763202123983937500,"lat":44.284989,"lon":8.915263,"alt":0,"pol":0,"mds":10832,"mcg":178,"status":1,"region":8}
{"time":1763202123983890000,"lat":44.27408,"lon":8.892988,"alt":0,"pol":0,"mds":7469,"mcg":197,"status":2,"region":9}
{"time":1763202123983890000,"lat":44.27408,"lon":8.892988,"alt":0,"pol":0,"mds":7469,"mcg":197,"status":1,"region":8}
{"time":1763202123983889200,"lat":44.279756,"lon":8.924568,"alt":0,"pol":0,"mds":7770,"mcg":82,"status":2,"region":9}
{"time":1763202123983889200,"lat":44.279756,"lon":8.924568,"alt":0,"pol":0,"mds":7770,"mcg":82,"status":1,"region":8}
{"time":1763202123983885800,"lat":44.276457,"lon":8.920456,"alt":0,"pol":0,"mds":5713,"mcg":159,"status":2,"region":9}
{"time":1763202123983885800,"lat":44.276457,"lon":8.920456,"alt":0,"pol":0,"mds":5713,"mcg":159,"status":1,"region":8}
{"time":1763202123702520300,"lat":-38.981925,"lon":151.55461,"alt":0,"pol":0,"mds":14906,"mcg":97,"status":0,"region":2}
{"time":1763202122363942000,"lat":-24.57577,"lon":148.610239,"alt":0,"pol":0,"mds":11794,"mcg":259,"status":0,"region":2}
{"time":1763202122363767300,"lat":-24.252154,"lon":148.741951,"alt":0,"pol":0,"mds":8582,"mcg":124,"status":0,"region":2}
{"time":1763202121942625800,"lat":24.982319,"lon":-59.714592,"alt":0,"pol":0,"mds":11143,"mcg":179,"status":0,"region":5}
{"time":1763202121942529000,"lat":24.770698,"lon":-59.515226,"alt":0,"pol":0,"mds":11771,"mcg":203,"status":1,"region":0}
{"time":1763202121942523000,"lat":24.785201,"lon":-59.502499,"alt":0,"pol":0,"mds":8231,"mcg":211,"status":0,"region":5}
{"time":1763202121735116800,"lat":-41.281446,"lon":152.188824,"alt":0,"pol":0,"mds":14916,"mcg":201,"status":0,"region":2}
{"time":1763202120567548200,"lat":-25.461695,"lon":149.862769,"alt":0,"pol":0,"mds":8506,"mcg":252,"status":0,"region":2}
{"time":1763202120439335400,"lat":-25.508039,"lon":149.845275,"alt":0,"pol":0,"mds":9619,"mcg":261,"status":0,"region":2}
{"time":1763202117207764500,"lat":36.865984,"lon":-9.198683,"alt":0,"pol":0,"mds":9862,"mcg":165,"status":0,"region":1}
{"time":1763202117194445000,"lat":36.918935,"lon":-9.123588,"alt":0,"pol":0,"mds":10229,"mcg":169,"status":0,"region":1}
{"time":1763202117194435300,"lat":36.959022,"lon":-9.187297,"alt":0,"pol":0,"mds":12547,"mcg":169,"status":2,"region":9}
{"time":1763202117194435300,"lat":36.959022,"lon":-9.187297,"alt":0,"pol":0,"mds":12547,"mcg":169,"status":1,"region":8}
{"time":1763202117194433500,"lat":36.845773,"lon":-9.083014,"alt":0,"pol":0,"mds":14208,"mcg":228,"status":0,"region":1}"""

@pytest.fixture
def data_request():
    with patch('blitzortung.cli.imprt2.requests.get') as mock_get:
        yield mock_get
