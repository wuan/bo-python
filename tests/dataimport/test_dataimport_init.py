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

from assertpy import assert_that

import blitzortung.dataimport
import blitzortung.dataimport.strike


class TestDataimportFactoryFunctions:

    def test_strikes_factory(self):
        assert_that(blitzortung.dataimport.strikes()).is_instance_of(blitzortung.dataimport.strike.StrikesBlitzortungDataProvider)
