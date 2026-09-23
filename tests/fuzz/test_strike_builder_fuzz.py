# -*- coding: utf8 -*-

"""

   Copyright 2025 Andreas Würl

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""

import datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from blitzortung.builder.base import BuilderError
from blitzortung.builder.strike import Strike
from blitzortung.data import Strike as StrikeData
from blitzortung.data import Timestamp
from tests.fuzz.support import FUZZ_SETTINGS

pytestmark = pytest.mark.fuzz

_JSON_VALUES = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(),
    st.lists(st.integers()),
)


@FUZZ_SETTINGS
@given(st.text())
def test_from_line_only_raises_builder_error(line):
    """Malformed text feed lines must surface as ``BuilderError`` only."""
    builder = Strike()
    try:
        builder.from_line(line)
    except BuilderError:
        pass


@FUZZ_SETTINGS
@given(st.dictionaries(st.text(), _JSON_VALUES, max_size=8))
def test_from_json_only_raises_builder_error(payload):
    """Malformed JSON payloads must surface as ``BuilderError`` only."""
    builder = Strike()
    try:
        builder.from_json(payload)
    except BuilderError:
        pass


@FUZZ_SETTINGS
@given(st.dictionaries(st.text(), _JSON_VALUES, max_size=8))
def test_from_json_build_only_raises_builder_error(payload):
    """A successfully parsed payload must always build a ``Strike``."""
    builder = Strike()
    try:
        strike = builder.from_json(payload).build()
    except BuilderError:
        return

    assert isinstance(strike, StrikeData)


@st.composite
def _valid_strike_lines(draw):
    """Generate a well-formed text feed line together with its parsed values."""
    date_time = draw(st.datetimes(
        min_value=datetime.datetime(1901, 1, 1),
        max_value=datetime.datetime(2100, 1, 1),
    ))
    nanosecond = draw(st.integers(min_value=0, max_value=999))
    latitude = draw(st.floats(min_value=-90, max_value=90, allow_nan=False, allow_infinity=False))
    longitude = draw(st.floats(min_value=-180, max_value=180, allow_nan=False, allow_infinity=False))
    altitude = draw(st.floats(min_value=-500, max_value=9000, allow_nan=False, allow_infinity=False))
    amplitude = draw(st.floats(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False))
    deviation = draw(st.floats(min_value=0, max_value=32767, allow_nan=False, allow_infinity=False))
    stations = draw(st.lists(st.integers(min_value=0, max_value=9999), max_size=12))

    timestamp = (
            date_time.strftime("%Y-%m-%d %H:%M:%S.")
            + f"{date_time.microsecond:06d}"
            + f"{nanosecond:03d}"
    )
    lat = f"{latitude:.6f}"
    lon = f"{longitude:.6f}"
    alt = f"{altitude:.6f}"
    amp = f"{amplitude:.6f}"
    dev = f"{deviation:.6f}"
    station_list = ",".join(str(station) for station in stations)

    line = (
        f"{timestamp} pos;{lat};{lon};{alt} str;{amp} dev;{dev} "
        f"sta;{len(stations)};0;{station_list}"
    )
    expected = {
        "timestamp": date_time.replace(tzinfo=datetime.timezone.utc),
        "nanosecond": nanosecond,
        "latitude": float(lat),
        "longitude": float(lon),
        "altitude": float(alt),
        "amplitude": float(amp),
        "lateral_error": float(dev),
        "station_count": len(stations),
        "stations": stations,
    }
    return line, expected


@FUZZ_SETTINGS
@given(_valid_strike_lines())
def test_valid_line_round_trip(case):
    """A well-formed line must be parsed back into the encoded values."""
    line, expected = case

    strike = Strike().from_line(line).build()

    assert strike.timestamp.datetime == expected["timestamp"]
    assert strike.timestamp.nanosecond == expected["nanosecond"]
    assert strike.x == pytest.approx(expected["longitude"])
    assert strike.y == pytest.approx(expected["latitude"])
    assert strike.altitude == pytest.approx(expected["altitude"])
    assert strike.amplitude == pytest.approx(expected["amplitude"])
    assert strike.lateral_error == pytest.approx(expected["lateral_error"])
    assert strike.station_count == expected["station_count"]
    assert strike.stations == expected["stations"]


@FUZZ_SETTINGS
@given(
    longitude=st.floats(min_value=-180, max_value=180, allow_nan=False, allow_infinity=False),
    latitude=st.floats(min_value=-90, max_value=90, allow_nan=False, allow_infinity=False),
    total_nanoseconds=st.integers(min_value=0, max_value=4102444800 * 10 ** 9),
    region=st.integers(min_value=0, max_value=9999),
)
def test_valid_json_round_trip(longitude, latitude, total_nanoseconds, region):
    """A well-formed websocket payload must be parsed back into the values."""
    strike = Strike().from_json({
        "lon": longitude,
        "lat": latitude,
        "time": total_nanoseconds,
        "region": region,
    }).build()

    assert strike.x == pytest.approx(round(longitude, 4))
    assert strike.y == pytest.approx(round(latitude, 4))
    assert strike.timestamp == Timestamp(total_nanoseconds)
    assert strike.region == region
