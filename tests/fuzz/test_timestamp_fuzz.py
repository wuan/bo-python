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

from blitzortung.data import Timestamp
from tests.fuzz.support import FUZZ_SETTINGS

pytestmark = pytest.mark.fuzz


@FUZZ_SETTINGS
@given(st.text())
def test_from_timestamp_never_raises(text):
    """Timestamp string parsing must be total for arbitrary input."""
    result = Timestamp.from_timestamp(text)

    assert isinstance(result, tuple)
    assert len(result) == 2


@FUZZ_SETTINGS
@given(st.text())
def test_timestamp_string_never_raises(text):
    """Constructing a ``Timestamp`` from arbitrary text must never raise."""
    Timestamp(text)


@FUZZ_SETTINGS
@given(st.integers())
def test_timestamp_integer_is_bounded(value):
    """Out-of-range integer timestamps must raise ``ValueError``, not overflow."""
    try:
        Timestamp(value)
    except ValueError:
        pass


@FUZZ_SETTINGS
@given(st.one_of(
    st.floats(allow_nan=True, allow_infinity=True),
    st.lists(st.integers()),
    st.dictionaries(st.text(), st.integers()),
    st.binary(),
))
def test_unsupported_timestamp_types_raise_value_error(value):
    """Values that are neither datetime, str nor int must be rejected."""
    with pytest.raises(ValueError):
        Timestamp(value)
