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

import os

from hypothesis import HealthCheck, settings

# The default keeps the property tests fast enough to run with every test
# invocation.  For longer fuzzing runs increase it, e.g.
# ``BLITZORTUNG_FUZZ_EXAMPLES=20000 poetry run pytest tests/fuzz``.
FUZZ_EXAMPLES = int(os.environ.get("BLITZORTUNG_FUZZ_EXAMPLES", "250"))

FUZZ_SETTINGS = settings(
    max_examples=FUZZ_EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
