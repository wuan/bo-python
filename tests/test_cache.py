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
from unittest import TestCase

from assertpy import assert_that
from mock import Mock

from blitzortung.cache import CacheEntry, ObjectCache


class TestCacheEntry(TestCase):
    def setUp(self):
        self.payload = Mock()
        self.cache_entry = CacheEntry(self.payload, time.time() + 10)

    def test_is_valid(self):
        assert_that(self.cache_entry.is_valid(time.time()))

        self.cache_entry = CacheEntry(self.payload, time.time() - 10)
        assert_that(not self.cache_entry.is_valid(time.time()))

    def test_get_payload(self):
        assert_that(self.cache_entry.get_payload()).is_equal_to(self.payload)

    def test_get_payload_increases_hit_count(self):
        self.cache_entry.get_payload()

        assert_that(self.cache_entry.get_hit_count()).is_equal_to(1)

    def test_get_hit_count(self):
        assert_that(self.cache_entry.get_hit_count()).is_equal_to(0)


class CachedObject:
    def __init__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs

    def get_args(self):
        return self.__args

    def get_kwargs(self):
        return self.__kwargs


class TestObjectCache(TestCase):
    def setUp(self):
        self.cache = ObjectCache()

    def test_get_time_to_live_default_value(self):
        assert_that(self.cache.get_time_to_live()).is_equal_to(30)

    def test_get_time_to_live(self):
        self.cache = ObjectCache(ttl_seconds=60)
        assert_that(self.cache.get_time_to_live()).is_equal_to(60)

    def test_get(self):
        cached_object = self.cache.get(CachedObject)
        assert_that(cached_object).is_instance_of(CachedObject)

    def test_get_caches_objects(self):
        cached_object = self.cache.get(CachedObject)
        assert_that(self.cache.get(CachedObject)).is_same_as(cached_object)

    def test_clear_clears_cache(self):
        cached_object = self.cache.get(CachedObject)

        self.cache.clear()
        assert_that(self.cache.get(CachedObject)).is_not_same_as(cached_object)

    def test_clear_resets_counters(self):
        self.cache.get(CachedObject)
        self.cache.get(CachedObject)

        self.cache.clear()

        assert_that(self.cache.get_ratio()).is_equal_to(0.0)

    def test_get_creates_new_object_if_original_object_is_expired(self):
        self.cache = ObjectCache(ttl_seconds=-10)
        cached_object = self.cache.get(CachedObject)
        assert_that(self.cache.get(CachedObject)).is_not_same_as(cached_object)

    def test_get_different_objects_for_different_create_objects(self):
        class OtherTestObject(CachedObject):
            pass

        cached_object = self.cache.get(CachedObject)
        other_cached_object = self.cache.get(OtherTestObject)

        assert_that(cached_object).is_not_equal_to(other_cached_object)

    def test_get_with_arg_is_called_with_same_arg(self):
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, argument1, argument2)

        assert_that(cached_object.get_args()).contains(argument1, argument2)
        assert_that(not cached_object.get_kwargs())

    def test_get_with_arg_is_cached(self):
        argument = object()

        cached_object = self.cache.get(CachedObject, argument)
        assert_that(self.cache.get(CachedObject, argument)).is_same_as(cached_object)

    def test_get_with_kwargs_is_called_with_same_kwargs(self):
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, foo=argument1, bar=argument2)

        assert_that(not cached_object.get_args())
        assert_that(cached_object.get_kwargs()).is_equal_to({'foo': argument1, 'bar': argument2})

    def test_get_with_kwargs_is_cached(self):
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, foo=argument1, bar=argument2)
        assert_that(self.cache.get(CachedObject, bar=argument2, foo=argument1)).is_same_as(cached_object)

    def test_get_with_kwargs_and_simlar_arg_and_is_not_cached(self):
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, foo=argument1, bar=argument2)
        assert_that(self.cache.get(CachedObject, ('bar', argument2), foo=argument1)).is_not_same_as(cached_object)

    def test_get_ratio(self):
        assert_that(self.cache.get_ratio()).is_equal_to(0.0)

        self.cache.get(CachedObject)
        assert_that(self.cache.get_ratio()).is_equal_to(0.0)

        self.cache.get(CachedObject)
        assert_that(self.cache.get_ratio()).is_equal_to(0.5)


class TestObjectCacheWithSize(TestCase):
    def setUp(self):
        self.cache = ObjectCache(ttl_seconds=1, size=2)

    def test_limited_size(self):
        foo_1 = self.cache.get(CachedObject, name="foo")
        assert_that(self.cache.get_size()).is_equal_to(1)
        self.cache.get(CachedObject, name="bar")
        assert_that(self.cache.get_size()).is_equal_to(2)
        self.cache.get(CachedObject, name="baz")
        assert_that(self.cache.get_size()).is_equal_to(2)
        foo_2 = self.cache.get(CachedObject, name="foo")
        assert_that(foo_1).is_not_same_as(foo_2)

    def test_track_recent_usage(self):
        foo_1 = self.cache.get(CachedObject, name="foo")
        bar = self.cache.get(CachedObject, name="bar")
        assert_that(list(self.cache.keys.values())).is_equal_to([0, 0])
        foo_2 = self.cache.get(CachedObject, name="foo")
        assert_that(list(self.cache.keys.values())).is_equal_to([0, 1])
        assert_that(foo_1).is_same_as(foo_2)

    def test_expiry(self):
        _ = self.cache.get(CachedObject, name="foo")
        self.cache.clean_expired()
        assert_that(self.cache.get_size()).is_equal_to(1)
        time.sleep(1)
        self.cache.clean_expired()
        assert_that(self.cache.get_size()).is_equal_to(0)

    def test_auto_expiry(self):
        self.cache = ObjectCache(ttl_seconds=1, size=2, cleanup_period=1)
        _ = self.cache.get(CachedObject, name="foo")
        assert_that(self.cache.get_size()).is_equal_to(1)
        time.sleep(1)
        _ = self.cache.get(CachedObject, name="bar")
        assert_that(self.cache.get_size()).is_equal_to(1)

def test_bench_object_cache_get(benchmark):
    cache = ObjectCache()
    benchmark.pedantic(cache.get, args=(CachedObject, "foo"), rounds=1000, iterations=100)

def test_bench_object_cache_with_size_get(benchmark):
    cache = ObjectCache(size=2)
    benchmark.pedantic(cache.get, args=(CachedObject, "foo"), rounds=1000, iterations=100)

def test_bench_object_cache_generate_cache_key(benchmark):
    cache = ObjectCache()
    benchmark.pedantic(cache.generate_cache_key, args=(CachedObject, ("foo", "bar"), {"baz": "asdf", "qux": "quux"}), rounds=1000, iterations=100)

    print("hit count", cache.total_hit_count)
