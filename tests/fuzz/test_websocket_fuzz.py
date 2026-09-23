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

import pytest
from hypothesis import given
from hypothesis import strategies as st

from blitzortung.websocket import decode
from tests.fuzz.support import FUZZ_SETTINGS

pytestmark = pytest.mark.fuzz


@FUZZ_SETTINGS
@given(st.text())
def test_decode_is_total_for_arbitrary_text(text):
    """The decoder consumes remote data and must never raise for any input."""
    result = decode(text)

    assert isinstance(result, str)


@FUZZ_SETTINGS
@given(st.binary())
def test_decode_harness_matches_decoder(data):
    """The raw-bytes harness used by the Atheris driver must be total."""
    result = decode(data.decode("utf-8", errors="ignore"))

    assert isinstance(result, str)
