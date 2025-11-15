import pytest

from blitzortung.gis.local_grid import LocalGrid


@pytest.mark.parametrize("data_area,x,y,ref_lon,ref_lat,center,extension", [
    (5,6,9,25,40,47.5,3.1666),
    (10, 0, 0, -10, -10, 5.0, 0.3333),
])
def test_local_grid(data_area, x, y, ref_lon, ref_lat, center, extension):
    uut = LocalGrid(data_area, x, y)

    assert uut.reference_longitude == ref_lon
    assert uut.reference_latitude == ref_lat
    assert uut.center_latitude == center
    assert uut.longitude_extension == pytest.approx(extension, rel=1e-3)

@pytest.mark.parametrize("data_area,x,y,ref_lon,ref_lat,center,extension", [
    (5,6,9,25,40,47.5,3.1666),
    (10, 0, 0, -10, -10, 5.0, 0.3333),
])
def test_local_grid(data_area, x, y, ref_lon, ref_lat, center, extension):
    uut = LocalGrid(data_area, x, y).get_grid_factory()

    grid = uut.get_for(10000)

    assert grid.x_min == pytest.approx(ref_lon - extension, rel=0.01)
    assert grid.x_max == pytest.approx(ref_lon + 3 * data_area + extension, rel=0.01)
    assert grid.y_min == ref_lat
    assert grid.y_max == pytest.approx(ref_lat + 3 * data_area, rel=0.1)
