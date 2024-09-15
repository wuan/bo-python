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

import time


class CacheEntry:
    def __init__(self, payload, expiry_time):
        self.__payload = payload
        self.__expiry_time = expiry_time
        self.__hit_count = 0

    def is_valid(self, current_time):
        return current_time < self.__expiry_time

    def get_payload(self):
        self.__hit_count += 1
        return self.__payload

    def get_hit_count(self):
        return self.__hit_count

    def __repr__(self):
        valid = "+" if self.is_valid(self.__hit_count) else "-"

        return f"cached<{valid} {self.get_hit_count()}>:{self.__payload}"


class ObjectCache:
    __KWA_MARK = object()

    def __init__(self, ttl_seconds=30, size=None):
        self.__ttl_seconds = int(ttl_seconds)
        self.total_count = 0
        self.total_hit_count = 0
        self.size = size

        self.cache = {}
        self.keys = {}

    def get(self, cached_object_creator, *args, **kwargs):
        self.total_count += 1

        cache_key = (cached_object_creator,) + args + (ObjectCache.__KWA_MARK,) \
                    + tuple(sorted(kwargs.items()))
        current_time = int(time.time())

        if cache_key in self.cache:
            count = 0
            if self.size is not None:
                count = self.keys[cache_key]
                del self.keys[cache_key]
            entry = self.cache[cache_key]
            if entry.is_valid(current_time):
                if self.size is not None:
                    self.keys[cache_key] = count + 1
                self.total_hit_count += 1
                return entry.get_payload()
        elif self.size is not None:
            if len(self.keys) >= self.size:
                expired_key = next(iter(self.keys))
                del self.keys[expired_key]
                del self.cache[expired_key]

        expires = current_time + self.__ttl_seconds
        payload = cached_object_creator(*args, **kwargs)

        entry = CacheEntry(payload, expires)
        self.cache[cache_key] = entry
        self.keys[cache_key] = 0

        return entry.get_payload()

    def clear(self):
        self.total_count = 0
        self.total_hit_count = 0
        self.cache.clear()

    def clean_expired(self):
        now = time.time()
        expired_keys = {key for key, entry in self.cache.items() if not entry.is_valid(now)}
        for expired_key in expired_keys:
            del self.keys[expired_key]
            del self.cache[expired_key]

    def get_time_to_live(self):
        return self.__ttl_seconds

    def get_ratio(self):
        if self.total_hit_count == 0:
            return 0.0
        return self.total_hit_count / self.total_count

    def get_size(self):
        return len(self.cache)