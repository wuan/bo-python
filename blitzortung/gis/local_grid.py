from dataclasses import dataclass

import blitzortung.geom
from .constants import UTM_NORTH, UTM_SOUTH


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
        return blitzortung.geom.GridFactory(
            self.reference_longitude - self.longitude_extension,
            self.reference_longitude + self.size + self.longitude_extension,
            self.reference_latitude,
            self.reference_latitude + self.size,
            UTM_NORTH if self.reference_latitude >= 0 else UTM_SOUTH,
            LOCAL_GRID_UTM_LONGITUDE,
            self.reference_latitude + self.size / 2.0
        )


DATA_AREA_SIZE_FACTOR = 3
LOCAL_GRID_UTM_LONGITUDE = 3
