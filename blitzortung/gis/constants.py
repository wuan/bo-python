import pyproj

import blitzortung.geom

UTM_EU = pyproj.CRS('epsg:32633')  # UTM 33 N / WGS84
UTM_NORTH_AMERICA = pyproj.CRS('epsg:32614')  # UTM 14 N / WGS84
UTM_CENTRAL_AMERICA = pyproj.CRS('epsg:32614')  # UTM 14 N / WGS84
UTM_SOUTH_AMERICA = pyproj.CRS('epsg:32720')  # UTM 20 S / WGS84
UTM_OCEANIA = pyproj.CRS('epsg:32755')  # UTM 55 S / WGS84
UTM_ASIA = pyproj.CRS('epsg:32650')  # UTM 50 N / WGS84
UTM_AFRICA = pyproj.CRS('epsg:32633')  # UTM 33 N / WGS84
UTM_NORTH = pyproj.CRS('epsg:32631')  # UTM 31 N / WGS84
UTM_SOUTH = pyproj.CRS('epsg:32731')  # UTM 31 S / WGS84

grid = {
    1: blitzortung.geom.GridFactory(-25, 57, 27, 72, UTM_EU),
    2: blitzortung.geom.GridFactory(110, 180, -50, 0, UTM_OCEANIA),
    3: blitzortung.geom.GridFactory(-140, -50, 10, 60, UTM_NORTH_AMERICA),
    4: blitzortung.geom.GridFactory(85, 150, -10, 60, UTM_ASIA),
    5: blitzortung.geom.GridFactory(-100, -30, -50, 20, UTM_SOUTH_AMERICA),
    6: blitzortung.geom.GridFactory(-20, 50, -40, 40, UTM_AFRICA),
    7: blitzortung.geom.GridFactory(-115, -50, 0, 30, UTM_CENTRAL_AMERICA)
}

global_grid = blitzortung.geom.GridFactory(-180, 180, -90, 90, UTM_EU, 11, 48)
