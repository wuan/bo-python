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

"""Smoke tests for the raw-bytes Atheris harnesses.

These make sure the harness logic used by the Atheris drivers is exercised
(and stays total) even on platforms where Atheris is not installed.
"""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from fuzz.harnesses import fuzz_decode, fuzz_strike_from_json, fuzz_strike_from_line
from tests.fuzz.support import FUZZ_SETTINGS

pytestmark = pytest.mark.fuzz


@FUZZ_SETTINGS
@given(st.binary())
def test_fuzz_decode_harness(data):
    fuzz_decode(data)


@FUZZ_SETTINGS
@given(st.binary())
def test_fuzz_strike_from_line_harness(data):
    fuzz_strike_from_line(data)


@FUZZ_SETTINGS
@given(st.binary())
def test_fuzz_strike_from_json_harness(data):
    fuzz_strike_from_json(data)
