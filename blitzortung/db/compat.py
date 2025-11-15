# -*- coding: utf8 -*-

"""

   Copyright 2014-2016 Andreas Würl

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

"""
Database compatibility layer for psycopg2cffi.

This module registers psycopg2cffi as a drop-in replacement for psycopg2,
which is useful for PyPy compatibility. The registration happens automatically
when this module is imported.
"""

try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass
