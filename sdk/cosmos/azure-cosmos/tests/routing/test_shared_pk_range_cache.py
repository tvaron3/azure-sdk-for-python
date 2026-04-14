# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

import sys
import unittest

import pytest

from azure.cosmos._routing.routing_range import Range
from azure.cosmos._routing.collection_routing_map import CollectionRoutingMap
from azure.cosmos._routing.routing_map_provider import (
    PartitionKeyRangeCache,
    _shared_routing_map_cache,
    _shared_cache_lock,
)


class MockClient:
    def __init__(self, url_connection):
        self.url_connection = url_connection


@pytest.mark.cosmosEmulator
class TestSharedPartitionKeyRangeCache(unittest.TestCase):

    def tearDown(self):
        with _shared_cache_lock:
            _shared_routing_map_cache.clear()

    def test_same_endpoint_shares_cache(self):
        c1 = MockClient("https://account1.documents.azure.com:443/")
        c2 = MockClient("https://account1.documents.azure.com:443/")
        cache1 = PartitionKeyRangeCache(c1)
        cache2 = PartitionKeyRangeCache(c2)
        self.assertIs(cache1._collection_routing_map_by_item,
                      cache2._collection_routing_map_by_item)

    def test_different_endpoints_isolated(self):
        c1 = MockClient("https://account1.documents.azure.com:443/")
        c2 = MockClient("https://account2.documents.azure.com:443/")
        cache1 = PartitionKeyRangeCache(c1)
        cache2 = PartitionKeyRangeCache(c2)
        self.assertIsNot(cache1._collection_routing_map_by_item,
                         cache2._collection_routing_map_by_item)

    def test_shared_cache_populated_by_first_client(self):
        c1 = MockClient("https://account1.documents.azure.com:443/")
        c2 = MockClient("https://account1.documents.azure.com:443/")
        cache1 = PartitionKeyRangeCache(c1)
        cache2 = PartitionKeyRangeCache(c2)
        pk_ranges = [{"id": "0", "minInclusive": "", "maxExclusive": "FF"}]
        crm = CollectionRoutingMap.CompleteRoutingMap(
            [(r, True) for r in pk_ranges], "test-collection"
        )
        cache1._collection_routing_map_by_item["test-collection"] = crm
        self.assertIn("test-collection", cache2._collection_routing_map_by_item)
        self.assertIs(cache2._collection_routing_map_by_item["test-collection"], crm)

    def test_clear_cache_resets_for_endpoint(self):
        c1 = MockClient("https://account1.documents.azure.com:443/")
        cache1 = PartitionKeyRangeCache(c1)
        cache1._collection_routing_map_by_item["coll1"] = "dummy"
        cache1.clear_cache()
        self.assertNotIn("coll1", cache1._collection_routing_map_by_item)

    def test_clear_cache_does_not_affect_other_endpoints(self):
        c1 = MockClient("https://account1.documents.azure.com:443/")
        c2 = MockClient("https://account2.documents.azure.com:443/")
        cache1 = PartitionKeyRangeCache(c1)
        cache2 = PartitionKeyRangeCache(c2)
        cache1._collection_routing_map_by_item["coll1"] = "data1"
        cache2._collection_routing_map_by_item["coll2"] = "data2"
        cache1.clear_cache()
        self.assertNotIn("coll1", cache1._collection_routing_map_by_item)
        self.assertIn("coll2", cache2._collection_routing_map_by_item)

    def test_range_has_slots(self):
        r = Range("00", "FF", True, False)
        self.assertFalse(hasattr(r, "__dict__"))
        self.assertLess(sys.getsizeof(r), 100)

    def test_range_skips_upper_when_already_uppercase(self):
        original = "05C1C9CD673398"
        r = Range(original, original, True, False)
        self.assertIs(r.min, original)

    def test_range_applies_upper_when_lowercase(self):
        r = Range("05c1c9cd", "05c1d9cd", True, False)
        self.assertEqual(r.min, "05C1C9CD")


if __name__ == "__main__":
    unittest.main()
