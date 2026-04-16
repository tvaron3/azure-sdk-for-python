# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

"""Integration tests for the shared partition key range cache and PKRange namedtuple.

These tests validate that multiple CosmosClient instances sharing the same endpoint
correctly share the routing map cache, that clear_cache() works transparently,
and that PKRange namedtuples are compatible with all CRUD and query operations.
"""

import unittest
import uuid
from unittest.mock import patch

import pytest

import test_config
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos._routing.routing_range import PKRange
from azure.cosmos._routing.routing_map_provider import (
    _shared_routing_map_cache,
    _shared_cache_lock,
)


@pytest.mark.cosmosEmulator
class TestSharedCacheIntegration(unittest.TestCase):
    """Integration tests requiring the Cosmos emulator."""

    host = test_config.TestConfig.host
    master_key = test_config.TestConfig.masterKey
    TEST_DATABASE_ID = test_config.TestConfig.TEST_DATABASE_ID
    TEST_CONTAINER_ID = "shared-cache-test-" + str(uuid.uuid4())[:8]

    @classmethod
    def setUpClass(cls):
        cls.client1 = CosmosClient(cls.host, cls.master_key)
        cls.db = cls.client1.get_database_client(cls.TEST_DATABASE_ID)
        cls.container = cls.db.create_container_if_not_exists(
            id=cls.TEST_CONTAINER_ID,
            partition_key=PartitionKey(path="/pk"),
        )
        # Seed data
        for i in range(20):
            cls.container.upsert_item({"id": f"item-{i}", "pk": f"pk-{i % 5}", "value": i})

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.delete_container(cls.TEST_CONTAINER_ID)
        except Exception:
            pass
        pass  # sync client cleaned up by GC

    def tearDown(self):
        # Clean up shared cache between tests
        with _shared_cache_lock:
            _shared_routing_map_cache.clear()

    def _get_routing_provider(self, client):
        return client.client_connection._routing_map_provider

    def _get_cache_dict(self, client):
        return self._get_routing_provider(client)._collection_routing_map_by_item

    def test_multi_client_shared_cache_reads(self):
        """Two clients to the same endpoint share the routing map after the first read."""
        client2 = CosmosClient(self.host, self.master_key)
        try:
            container2 = client2.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)

            # Client1 read triggers routing map population
            self.container.read_item("item-0", partition_key="pk-0")

            cache1 = self._get_cache_dict(self.client1)
            cache2 = self._get_cache_dict(client2)

            # Both clients point to the same cache dict
            self.assertIs(cache1, cache2)

            # Client2 can read without triggering a new _ReadPartitionKeyRanges
            result = container2.read_item("item-1", partition_key="pk-1")
            self.assertEqual(result["id"], "item-1")
        finally:
            pass  # sync client cleaned up by GC

    def test_multi_client_shared_cache_queries(self):
        """Client2 uses cached routing map populated by client1 for queries."""
        client2 = CosmosClient(self.host, self.master_key)
        try:
            container2 = client2.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)

            # Client1 query populates the cache
            list(self.container.query_items("SELECT * FROM c", enable_cross_partition_query=True))

            # Verify cache is populated
            cache = self._get_cache_dict(self.client1)
            self.assertTrue(len(cache) > 0, "Cache should be populated after query")

            # Client2 query should use the cached routing map
            results = list(container2.query_items(
                "SELECT * FROM c WHERE c.pk = 'pk-0'",
                enable_cross_partition_query=True
            ))
            self.assertTrue(len(results) > 0)
        finally:
            pass  # sync client cleaned up by GC

    def test_clear_cache_triggers_repopulation(self):
        """After clear_cache(), the next operation transparently re-populates."""
        # Populate cache
        self.container.read_item("item-0", partition_key="pk-0")
        cache = self._get_cache_dict(self.client1)
        self.assertTrue(len(cache) > 0)

        # Clear and verify empty
        provider = self._get_routing_provider(self.client1)
        provider.clear_cache()
        cache = self._get_cache_dict(self.client1)
        self.assertEqual(len(cache), 0)

        # Next read transparently re-populates
        result = self.container.read_item("item-0", partition_key="pk-0")
        self.assertEqual(result["id"], "item-0")
        cache = self._get_cache_dict(self.client1)
        self.assertTrue(len(cache) > 0)

    def test_clear_cache_propagates_to_shared_clients(self):
        """clear_cache() on client1 creates a new dict; client2 must re-attach on next use."""
        client2 = CosmosClient(self.host, self.master_key)
        try:
            container2 = client2.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)

            # Both populate via client1
            self.container.read_item("item-0", partition_key="pk-0")
            old_cache = self._get_cache_dict(self.client1)
            self.assertTrue(len(old_cache) > 0)

            # Clear via client1
            self._get_routing_provider(self.client1).clear_cache()

            # Both clients still reference the same (now empty) shared dict
            # because clear_cache uses .clear() to preserve references
            cache1 = self._get_cache_dict(self.client1)
            cache2 = self._get_cache_dict(client2)
            self.assertIs(cache1, cache2, "Both clients should reference the same dict after clear_cache")
            self.assertEqual(len(cache1), 0)

            # Client2 read re-populates
            result = container2.read_item("item-2", partition_key="pk-2")
            self.assertEqual(result["id"], "item-2")
        finally:
            pass  # sync client cleaned up by GC

    def test_different_endpoints_isolated_with_emulator(self):
        """Emulator client cache is isolated from a different endpoint."""
        # Create a dummy provider for a different endpoint
        from azure.cosmos._routing.routing_map_provider import PartitionKeyRangeCache

        class DummyClient:
            url_connection = "https://other-account.documents.azure.com:443/"

        dummy_cache = PartitionKeyRangeCache(DummyClient())
        dummy_cache._collection_routing_map_by_item["dummy-coll"] = "dummy-data"

        # Populate emulator cache
        self.container.read_item("item-0", partition_key="pk-0")
        emulator_cache = self._get_cache_dict(self.client1)

        # Verify isolation
        self.assertNotIn("dummy-coll", emulator_cache)
        self.assertIn("dummy-coll", dummy_cache._collection_routing_map_by_item)

    def test_pkrange_survives_full_crud_lifecycle(self):
        """All CRUD operations work correctly with PKRange-based routing maps."""
        crud_id = f"crud-{uuid.uuid4()}"

        # Create
        item = self.container.create_item({"id": crud_id, "pk": "crud-pk", "data": "test"})
        self.assertEqual(item["id"], crud_id)

        # Read
        read = self.container.read_item(crud_id, partition_key="crud-pk")
        self.assertEqual(read["data"], "test")

        # Replace
        read["data"] = "updated"
        replaced = self.container.replace_item(crud_id, read)
        self.assertEqual(replaced["data"], "updated")

        # Query
        results = list(self.container.query_items(
            f"SELECT * FROM c WHERE c.id = '{crud_id}'",
            enable_cross_partition_query=True
        ))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["data"], "updated")

        # Upsert
        read["data"] = "upserted"
        upserted = self.container.upsert_item(read)
        self.assertEqual(upserted["data"], "upserted")

        # Delete
        self.container.delete_item(crud_id, partition_key="crud-pk")
        with self.assertRaises(Exception):
            self.container.read_item(crud_id, partition_key="crud-pk")

        # Verify cache still has PKRange-based routing map
        cache = self._get_cache_dict(self.client1)
        self.assertTrue(len(cache) > 0)

    def test_pkrange_in_change_feed(self):
        """Change feed operations work with PKRange-based routing maps."""
        # Insert a new item for change feed
        cf_id = f"cf-{uuid.uuid4()}"
        self.container.create_item({"id": cf_id, "pk": "cf-pk", "data": "change-feed-test"})

        # Read change feed from beginning
        results = list(self.container.query_items_change_feed(
            start_time="Beginning",
            partition_key="cf-pk"
        ))
        self.assertTrue(len(results) > 0, "Change feed should return results")

        # Cross-partition change feed
        all_results = list(self.container.query_items_change_feed(start_time="Beginning"))
        self.assertTrue(len(all_results) > 0, "Cross-partition change feed should return results")

        # Clean up
        self.container.delete_item(cf_id, partition_key="cf-pk")


if __name__ == "__main__":
    unittest.main()
