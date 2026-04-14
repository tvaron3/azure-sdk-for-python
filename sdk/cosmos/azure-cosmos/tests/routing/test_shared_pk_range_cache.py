# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

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
    """Minimal mock client for PartitionKeyRangeCache tests."""
    def __init__(self, url_connection):
        self.url_connection = url_connection


@pytest.mark.cosmosEmulator
class TestSharedPartitionKeyRangeCache(unittest.TestCase):

    def tearDown(self):
        # Clean up shared cache between tests
        with _shared_cache_lock:
            _shared_routing_map_cache.clear()

    def test_same_endpoint_shares_cache(self):
        """Two clients with the same endpoint share the same routing map dict."""
        client1 = MockClient("https://account1.documents.azure.com:443/")
        client2 = MockClient("https://account1.documents.azure.com:443/")

        cache1 = PartitionKeyRangeCache(client1)
        cache2 = PartitionKeyRangeCache(client2)

        self.assertIs(cache1._collection_routing_map_by_item,
                      cache2._collection_routing_map_by_item)

    def test_different_endpoints_isolated(self):
        """Two clients with different endpoints have separate caches."""
        client1 = MockClient("https://account1.documents.azure.com:443/")
        client2 = MockClient("https://account2.documents.azure.com:443/")

        cache1 = PartitionKeyRangeCache(client1)
        cache2 = PartitionKeyRangeCache(client2)

        self.assertIsNot(cache1._collection_routing_map_by_item,
                         cache2._collection_routing_map_by_item)

    def test_shared_cache_populated_by_first_client(self):
        """When first client populates the cache, second client sees it."""
        client1 = MockClient("https://account1.documents.azure.com:443/")
        client2 = MockClient("https://account1.documents.azure.com:443/")

        cache1 = PartitionKeyRangeCache(client1)
        cache2 = PartitionKeyRangeCache(client2)

        # Simulate first client populating the routing map
        pk_ranges = [
            {"id": "0", "minInclusive": "", "maxExclusive": "FF"},
        ]
        crm = CollectionRoutingMap.CompleteRoutingMap(
            [(r, True) for r in pk_ranges], "test-collection"
        )
        cache1._collection_routing_map_by_item["test-collection"] = crm

        # Second client should see it
        self.assertIn("test-collection", cache2._collection_routing_map_by_item)
        self.assertIs(cache2._collection_routing_map_by_item["test-collection"], crm)

    def test_clear_cache_resets_for_endpoint(self):
        """clear_cache resets the shared entry for the endpoint."""
        client1 = MockClient("https://account1.documents.azure.com:443/")
        cache1 = PartitionKeyRangeCache(client1)

        cache1._collection_routing_map_by_item["coll1"] = "dummy"
        self.assertIn("coll1", cache1._collection_routing_map_by_item)

        cache1.clear_cache()
        self.assertNotIn("coll1", cache1._collection_routing_map_by_item)

    def test_clear_cache_does_not_affect_other_endpoints(self):
        """Clearing cache for one endpoint leaves other endpoints intact."""
        client1 = MockClient("https://account1.documents.azure.com:443/")
        client2 = MockClient("https://account2.documents.azure.com:443/")

        cache1 = PartitionKeyRangeCache(client1)
        cache2 = PartitionKeyRangeCache(client2)

        cache1._collection_routing_map_by_item["coll1"] = "data1"
        cache2._collection_routing_map_by_item["coll2"] = "data2"

        cache1.clear_cache()

        self.assertNotIn("coll1", cache1._collection_routing_map_by_item)
        self.assertIn("coll2", cache2._collection_routing_map_by_item)


if __name__ == "__main__":
    unittest.main()
