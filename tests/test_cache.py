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

from assertpy import assert_that
from mock import Mock
import pytest

from blitzortung.cache import CacheEntry, ObjectCache
from blitzortung.service.cache import ServiceCache


class TestCacheEntry:
    """Test suite for CacheEntry class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.payload = Mock()
        self.cache_entry = CacheEntry(self.payload, time.time() + 10)

    def test_is_valid(self):
        """Test cache entry validity check."""
        assert_that(self.cache_entry.is_valid(time.time())).is_true()

        self.cache_entry = CacheEntry(self.payload, time.time() - 10)
        assert_that(self.cache_entry.is_valid(time.time())).is_false()

    def test_get_payload(self):
        """Test getting payload from cache entry."""
        assert_that(self.cache_entry.get_payload()).is_equal_to(self.payload)

    def test_get_payload_increases_hit_count(self):
        """Test that getting payload increments hit count."""
        self.cache_entry.get_payload()
        assert_that(self.cache_entry.get_hit_count()).is_equal_to(1)

    def test_get_hit_count(self):
        """Test getting initial hit count."""
        assert_that(self.cache_entry.get_hit_count()).is_equal_to(0)

    def test_repr_valid_entry(self):
        """Test string representation of valid cache entry."""
        # Use deterministic large expiry time (100 > initial hit_count of 0)
        self.cache_entry = CacheEntry("payload", 100)
        result = repr(self.cache_entry)
        assert_that(result).contains("cached<+")
        assert_that(result).contains("payload")

    def test_repr_with_hit_count(self):
        """Test string representation after retrieving payload (increases hit count)."""
        # Use expiry time of 1, after 2 hits hit_count=2 > expiry_time=1
        # This tests that invalid entries (expired) show "-" in repr
        self.cache_entry = CacheEntry("payload", 1)
        _ = self.cache_entry.get_payload()
        _ = self.cache_entry.get_payload()
        result = repr(self.cache_entry)
        assert_that(result).contains("cached<-")  # hit_count (2) > expiry_time (1)
        assert_that(result).contains("payload")
        assert_that(result).contains("2")  # hit count


class CachedObject:
    """Helper class for cache testing."""

    def __init__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs

    def get_args(self):
        """Get positional arguments."""
        return self.__args

    def get_kwargs(self):
        """Get keyword arguments."""
        return self.__kwargs


class TestObjectCache:
    """Test suite for ObjectCache class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.cache = ObjectCache()

    def test_get_time_to_live_default_value(self):
        """Test default TTL value."""
        assert_that(self.cache.get_time_to_live()).is_equal_to(30)

    def test_get_time_to_live(self):
        """Test custom TTL value."""
        self.cache = ObjectCache(ttl_seconds=60)
        assert_that(self.cache.get_time_to_live()).is_equal_to(60)

    def test_get(self):
        """Test getting cached object."""
        cached_object = self.cache.get(CachedObject)
        assert_that(cached_object).is_instance_of(CachedObject)

    def test_get_caches_objects(self):
        """Test that objects are cached and reused."""
        cached_object = self.cache.get(CachedObject)
        assert_that(self.cache.get(CachedObject)).is_same_as(cached_object)

    def test_clear_clears_cache(self):
        """Test clearing cache."""
        cached_object = self.cache.get(CachedObject)

        self.cache.clear()
        assert_that(self.cache.get(CachedObject)).is_not_same_as(cached_object)

    def test_clear_resets_counters(self):
        """Test that clearing cache resets counters."""
        self.cache.get(CachedObject)
        self.cache.get(CachedObject)

        self.cache.clear()

        assert_that(self.cache.get_ratio()).is_equal_to(0.0)

    def test_get_creates_new_object_if_original_object_is_expired(self):
        """Test creating new object when cached object expires."""
        self.cache = ObjectCache(ttl_seconds=-10)
        cached_object = self.cache.get(CachedObject)
        assert_that(self.cache.get(CachedObject)).is_not_same_as(cached_object)

    def test_get_different_objects_for_different_create_objects(self):
        """Test different objects for different classes."""

        class OtherTestObject(CachedObject):  # pylint: disable=missing-class-docstring
            pass

        cached_object = self.cache.get(CachedObject)
        other_cached_object = self.cache.get(OtherTestObject)

        assert_that(cached_object).is_not_equal_to(other_cached_object)

    def test_get_with_arg_is_called_with_same_arg(self):
        """Test caching with positional arguments."""
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, argument1, argument2)

        assert_that(cached_object.get_args()).contains(argument1, argument2)
        assert_that(not cached_object.get_kwargs())

    def test_get_with_arg_is_cached(self):
        """Test caching with positional arguments is reused."""
        argument = object()

        cached_object = self.cache.get(CachedObject, argument)
        assert_that(self.cache.get(CachedObject, argument)).is_same_as(cached_object)

    def test_get_with_kwargs_is_called_with_same_kwargs(self):
        """Test caching with keyword arguments."""
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, foo=argument1, bar=argument2)

        assert_that(not cached_object.get_args())
        assert_that(cached_object.get_kwargs()).is_equal_to(
            {"foo": argument1, "bar": argument2}
        )

    def test_get_with_kwargs_is_cached(self):
        """Test caching with keyword arguments is reused."""
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, foo=argument1, bar=argument2)
        assert_that(
            self.cache.get(CachedObject, bar=argument2, foo=argument1)
        ).is_same_as(cached_object)

    def test_get_with_kwargs_and_simlar_arg_and_is_not_cached(self):
        """Test different cache keys for similar args."""
        argument1 = object()
        argument2 = object()

        cached_object = self.cache.get(CachedObject, foo=argument1, bar=argument2)
        assert_that(
            self.cache.get(CachedObject, ("bar", argument2), foo=argument1)
        ).is_not_same_as(cached_object)

    def test_get_ratio(self):
        """Test cache hit ratio calculation."""
        assert_that(self.cache.get_ratio()).is_equal_to(0.0)

        self.cache.get(CachedObject)
        assert_that(self.cache.get_ratio()).is_equal_to(0.0)

        self.cache.get(CachedObject)
        assert_that(self.cache.get_ratio()).is_equal_to(0.5)


class TestObjectCacheWithSize:
    """Test suite for ObjectCache with size limit."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.cache = ObjectCache(ttl_seconds=1, size=2)

    def test_limited_size(self):
        """Test cache size limit enforcement."""
        foo_1 = self.cache.get(CachedObject, name="foo")
        assert_that(self.cache.get_size()).is_equal_to(1)
        self.cache.get(CachedObject, name="bar")
        assert_that(self.cache.get_size()).is_equal_to(2)
        self.cache.get(CachedObject, name="baz")
        assert_that(self.cache.get_size()).is_equal_to(2)
        foo_2 = self.cache.get(CachedObject, name="foo")
        assert_that(foo_1).is_not_same_as(foo_2)

    def test_track_recent_usage(self):
        """Test that recent usage is tracked."""
        foo_1 = self.cache.get(CachedObject, name="foo")
        self.cache.get(CachedObject, name="bar")
        assert_that(list(self.cache.keys.values())).is_equal_to([0, 0])
        foo_2 = self.cache.get(CachedObject, name="foo")
        assert_that(list(self.cache.keys.values())).is_equal_to([0, 1])
        assert_that(foo_1).is_same_as(foo_2)

    def test_expiry(self):
        """Test cache expiry."""
        _ = self.cache.get(CachedObject, name="foo")
        self.cache.clean_expired()
        assert_that(self.cache.get_size()).is_equal_to(1)
        time.sleep(1)
        self.cache.clean_expired()
        assert_that(self.cache.get_size()).is_equal_to(0)

    def test_auto_expiry(self):
        """Test automatic cache expiry."""
        self.cache = ObjectCache(ttl_seconds=1, size=2, cleanup_period=1)
        _ = self.cache.get(CachedObject, name="foo")
        assert_that(self.cache.get_size()).is_equal_to(1)
        time.sleep(1)
        _ = self.cache.get(CachedObject, name="bar")
        assert_that(self.cache.get_size()).is_equal_to(1)


def test_bench_object_cache_get(benchmark):
    """Benchmark cache.get() performance."""
    cache = ObjectCache()
    benchmark.pedantic(
        cache.get, args=(CachedObject, "foo"), rounds=1000, iterations=100
    )


def test_bench_object_cache_with_size_get(benchmark):
    """Benchmark cache.get() with size limit."""
    cache = ObjectCache(size=2)
    benchmark.pedantic(
        cache.get, args=(CachedObject, "foo"), rounds=1000, iterations=100
    )


def test_bench_object_cache_generate_cache_key(benchmark):
    """Benchmark cache key generation."""
    cache = ObjectCache()
    benchmark.pedantic(
        cache.generate_cache_key,
        args=(CachedObject, ("foo", "bar"), {"baz": "asdf", "qux": "quux"}),
        rounds=1000,
        iterations=100,
    )

    print("hit count", cache.total_hit_count)


class TestServiceCache:
    """Test suite for ServiceCache class."""

    def test_default_ttl_values(self):
        """Test default TTL values."""
        service_cache = ServiceCache()

        assert_that(service_cache.CACHE_CLEANUP_PERIOD).is_equal_to(300)
        assert_that(service_cache.CACHE_TTL_SHORT).is_equal_to(20)
        assert_that(service_cache.CACHE_TTL_LONG).is_equal_to(60)
        assert_that(service_cache.LOCAL_CACHE_SIZE_CURRENT).is_equal_to(100)
        assert_that(service_cache.LOCAL_CACHE_SIZE_HISTORY).is_equal_to(400)

    def test_global_strikes_current(self):
        """Test global_strikes returns current cache for minute_offset=0."""
        service_cache = ServiceCache()
        cache = service_cache.global_strikes(0)
        assert_that(cache).is_not_none()
        assert_that(cache.get_time_to_live()).is_equal_to(20)

    def test_global_strikes_history(self):
        """Test global_strikes returns history cache for minute_offset>0."""
        service_cache = ServiceCache()
        cache = service_cache.global_strikes(60)
        assert_that(cache).is_not_none()
        assert_that(cache.get_time_to_live()).is_equal_to(60)

    def test_local_strikes_current(self):
        """Test local_strikes returns current cache for minute_offset=0."""
        service_cache = ServiceCache()
        cache = service_cache.local_strikes(0)
        assert_that(cache).is_not_none()
        assert_that(cache.get_time_to_live()).is_equal_to(20)

    def test_local_strikes_history(self):
        """Test local_strikes returns history cache for minute_offset>0."""
        service_cache = ServiceCache()
        cache = service_cache.local_strikes(60)
        assert_that(cache).is_not_none()
        assert_that(cache.get_time_to_live()).is_equal_to(60)

    def test_strikes_current(self):
        """Test strikes returns current cache for minute_offset=0."""
        service_cache = ServiceCache()
        cache = service_cache.strikes(0)
        assert_that(cache).is_not_none()
        assert_that(cache.get_time_to_live()).is_equal_to(20)

    def test_strikes_history(self):
        """Test strikes returns history cache for minute_offset>0."""
        service_cache = ServiceCache()
        cache = service_cache.strikes(60)
        assert_that(cache).is_not_none()
        assert_that(cache.get_time_to_live()).is_equal_to(60)

    def test_histogram_cache(self):
        """Test histogram cache exists and has correct TTL."""
        service_cache = ServiceCache()
        cache = service_cache.histogram
        assert_that(cache).is_not_none()
        assert_that(cache.get_time_to_live()).is_equal_to(60)

    def test_different_caches_for_different_methods(self):
        """Test that different cache methods return different caches."""
        service_cache = ServiceCache()

        # All should be different objects
        assert_that(service_cache.strikes(0)).is_not_same_as(service_cache.global_strikes(0))
        assert_that(service_cache.strikes(0)).is_not_same_as(service_cache.local_strikes(0))
        assert_that(service_cache.global_strikes(0)).is_not_same_as(service_cache.local_strikes(0))
