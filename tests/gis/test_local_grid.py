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


def test_get_grid_factory_is_cached():
    """Grid factories are shared for identical local grid parameters."""
    first = LocalGrid(5, 6, 9).get_grid_factory()
    second = LocalGrid(5, 6, 9).get_grid_factory()
    other = LocalGrid(5, 6, 10).get_grid_factory()

    assert first is second
    assert first is not other


def test_cached_grid_factory_reuses_computed_grid():
    """A cached factory must return the same grid instance for a base length."""
    first = LocalGrid(5, 6, 9).get_grid_factory().get_for(10000)
    second = LocalGrid(5, 6, 9).get_grid_factory().get_for(10000)

    assert first is second
