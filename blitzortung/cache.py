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
    kwargs_separator = object()

    def __init__(self, ttl_seconds=30, size=None, cleanup_period=None):
        self.__ttl_seconds = int(ttl_seconds)
        self.total_count = 0
        self.total_hit_count = 0
        self.size = size

        self.cache = {}
        self.keys = {}
        self.last_cleanup = 0
        self.cleanup_period = cleanup_period

    def get(self, cached_object_creator, *args, **kwargs):
        if self.cleanup_period is not None:
            now = time.time()
            if now > self.last_cleanup + self.cleanup_period:
                before = self.get_size()
                self.clean_expired()
                after = self.get_size()
                print(f"{cached_object_creator.__name__}: cache cleanup {before} -> {after}")
                self.last_cleanup = now

        self.total_count += 1

        cache_key = self.generate_cache_key(cached_object_creator, args, kwargs)

        current_time = int(time.time())

        if cache_key in self.cache:
            entry = self.cache[cache_key]
            if entry.is_valid(current_time):
                if self.size is not None:
                    self.track_usage(cache_key)
                self.total_hit_count += 1
                return entry.get_payload()
        elif self.size is not None and len(self.keys) >= self.size:
            self.remove_oldest_entry()

        expires = current_time + self.__ttl_seconds
        payload = cached_object_creator(*args, **kwargs)

        entry = CacheEntry(payload, expires)
        self.cache[cache_key] = entry
        self.keys[cache_key] = 0

        return entry.get_payload()

    def remove_oldest_entry(self):
        expired_key = next(iter(self.keys))
        del self.keys[expired_key]
        del self.cache[expired_key]

    def track_usage(self, cache_key):
        count = self.keys[cache_key]
        del self.keys[cache_key]
        self.keys[cache_key] = count + 1

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


    def generate_cache_key(self, cached_object_creator, args, kwargs):
        """
        Generates a cache key based on the cached object creator function, args, and kwargs.

        :param cached_object_creator: The function used to create the object to be cached.
        :param args: Positional arguments to the object creator.
        :param kwargs: Keyword arguments to the object creator.
        :return: A tuple representing the cache key.
        """
        return (cached_object_creator,) + args + (self.kwargs_separator,) + tuple(sorted(kwargs.items()))
