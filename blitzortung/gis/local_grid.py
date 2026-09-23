from dataclasses import dataclass
from functools import lru_cache

import blitzortung.geom
from .constants import UTM_NORTH, UTM_SOUTH


DATA_AREA_SIZE_FACTOR = 3
LOCAL_GRID_UTM_LONGITUDE = 3


@dataclass
class LocalGrid:
    data_area: int
    x: int
    y: int

    @property
    def size(self):
        return self.data_area * DATA_AREA_SIZE_FACTOR

    @property
    def reference_longitude(self):
        return (self.x - 1) * self.data_area

    @property
    def reference_latitude(self):
        return (self.y - 1) * self.data_area

    @property
    def center_latitude(self):
        return self.reference_latitude + self.size / 2.0

    @property
    def longitude_extension(self):
        return abs(self.center_latitude) / 15.0

    def get_grid_factory(self) -> blitzortung.geom.GridFactory:
        return _create_grid_factory(self.data_area, self.x, self.y)


@lru_cache(maxsize=1024)
def _create_grid_factory(data_area: int, x: int, y: int) -> blitzortung.geom.GridFactory:
    """Return a cached grid factory for the given local grid parameters.

    Local grids are derived purely from ``data_area``/``x``/``y`` and the
    webservice used to rebuild the factory (and its coordinate transformers)
    on every request.  Caching the factory keeps the computed grids around
    between requests.
    """
    local_grid = LocalGrid(data_area, x, y)
    return blitzortung.geom.GridFactory(
        local_grid.reference_longitude - local_grid.longitude_extension,
        local_grid.reference_longitude + local_grid.size + local_grid.longitude_extension,
        local_grid.reference_latitude,
        local_grid.reference_latitude + local_grid.size,
        UTM_NORTH if local_grid.reference_latitude >= 0 else UTM_SOUTH,
        LOCAL_GRID_UTM_LONGITUDE,
        local_grid.reference_latitude + local_grid.size / 2.0,
    )
