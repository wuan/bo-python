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

"""Reusable entry points shared by the Atheris fuzz drivers.

Every harness accepts raw ``bytes`` so it can be called directly by the
fuzzer.  Expected failures caused by malformed input (``BuilderError``,
invalid JSON, undecodable text) are swallowed; anything else is allowed to
propagate so that the fuzzer reports it as a crash.

The functions are also exercised by ``tests/fuzz/test_atheris_harness.py``
so their behaviour is verified even where Atheris cannot be installed.
"""

import json
from typing import Any

from blitzortung.builder.base import BuilderError
from blitzortung.builder.strike import Strike
from blitzortung.websocket import decode


def decode_bytes(data: bytes) -> str:
    """Decode raw fuzzer bytes the same way the websocket importer does."""
    return data.decode("utf-8", errors="ignore")


def fuzz_decode(data: bytes) -> None:
    """Exercise the websocket payload decoder."""
    decode(decode_bytes(data))


def fuzz_strike_from_line(data: bytes) -> None:
    """Exercise the plain-text strike line parser."""
    try:
        Strike().from_line(decode_bytes(data)).build()
    except BuilderError:
        pass


def fuzz_strike_from_json(data: bytes) -> None:
    """Exercise the websocket JSON strike parser."""
    payload: Any
    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return

    try:
        Strike().from_json(payload).build()
    except BuilderError:
        pass
