# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

"""Fault injection tests for the shared partition key range cache.

These tests use FaultInjectionTransport to simulate failures (410 Gone,
partition splits, transient errors) and validate that the shared cache
correctly refreshes, serializes concurrent refreshes, and maintains
data integrity under concurrent access.
"""

import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

import pytest

import test_config
from _fault_injection_transport import FaultInjectionTransport
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos._routing.routing_range import PKRange
from azure.cosmos._routing.routing_map_provider import (
    PartitionKeyRangeCache,
    _shared_routing_map_cache,
    _shared_cache_lock,
)
from azure.cosmos.exceptions import CosmosHttpResponseError


@pytest.mark.cosmosEmulator
class TestSharedCacheFaultInjection(unittest.TestCase):
    """Fault injection tests requiring the Cosmos emulator."""

    host = test_config.TestConfig.host
    master_key = test_config.TestConfig.masterKey
    TEST_DATABASE_ID = test_config.TestConfig.TEST_DATABASE_ID
    TEST_CONTAINER_ID = "fault-cache-test-" + str(uuid.uuid4())[:8]

    @classmethod
    def setUpClass(cls):
        cls.client = CosmosClient(cls.host, cls.master_key)
        cls.db = cls.client.get_database_client(cls.TEST_DATABASE_ID)
        cls.container = cls.db.create_container_if_not_exists(
            id=cls.TEST_CONTAINER_ID,
            partition_key=PartitionKey(path="/pk"),
        )
        for i in range(10):
            cls.container.upsert_item({"id": f"fi-{i}", "pk": f"pk-{i % 3}", "value": i})

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.delete_container(cls.TEST_CONTAINER_ID)
        except Exception:
            pass
        cls.client.close()

    def tearDown(self):
        with _shared_cache_lock:
            _shared_routing_map_cache.clear()

    def _make_fault_client(self, transport):
        return CosmosClient(self.host, self.master_key, transport=transport)

    def test_gone_410_triggers_cache_refresh(self):
        """A 410 Gone error triggers cache refresh via clear_cache, and retry succeeds."""
        transport = FaultInjectionTransport()
        gone_error = CosmosHttpResponseError(
            status_code=410,
            message="Partition has moved.",
            sub_status=1002
        )
        call_count = {"pkranges": 0}
        original_send = transport.send

        def counting_send(request, **kwargs):
            if "pkranges" in request.url:
                call_count["pkranges"] += 1
            return original_send(request, **kwargs)

        # Inject Gone on first document read only
        is_document_read = lambda r: (
            FaultInjectionTransport.predicate_is_document_operation(r)
            and r.method == "GET"
        )
        transport.add_fault(
            predicate=is_document_read,
            fault_factory=lambda r: FaultInjectionTransport.error_after_delay(0, gone_error),
            max_inner_count=1,
        )

        client = self._make_fault_client(transport)
        try:
            container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)

            # This should trigger a 410, which causes cache refresh, then retry
            result = container.read_item("fi-0", partition_key="pk-0")
            self.assertEqual(result["id"], "fi-0")
        finally:
            client.close()

    def test_stale_cache_after_partition_split_simulation(self):
        """410/1002 (partition split) triggers routing map refresh, shared with client2."""
        transport = FaultInjectionTransport()
        split_error = CosmosHttpResponseError(
            status_code=410,
            message="Partition key range is gone.",
            sub_status=1002  # Partition split
        )

        is_document_read = lambda r: (
            FaultInjectionTransport.predicate_is_document_operation(r)
            and r.method == "GET"
        )
        transport.add_fault(
            predicate=is_document_read,
            fault_factory=lambda r: FaultInjectionTransport.error_after_delay(0, split_error),
            max_inner_count=1,
        )

        client1 = self._make_fault_client(transport)
        client2 = CosmosClient(self.host, self.master_key)
        try:
            container1 = client1.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)

            # Trigger split error on client1 -> cache refreshed
            result = container1.read_item("fi-1", partition_key="pk-1")
            self.assertEqual(result["id"], "fi-1")

            # Client2 should share the refreshed cache
            container2 = client2.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)
            result2 = container2.read_item("fi-2", partition_key="pk-2")
            self.assertEqual(result2["id"], "fi-2")

            # Both should point to the same shared cache
            cache1 = client1.client_connection._routing_map_provider._collection_routing_map_by_item
            cache2 = client2.client_connection._routing_map_provider._collection_routing_map_by_item
            self.assertIs(cache1, cache2)
        finally:
            client1.close()
            client2.close()

    def test_concurrent_cache_refresh_no_crash(self):
        """Multiple threads calling clear_cache + read concurrently don't crash or corrupt."""
        errors = []

        def worker(worker_id):
            try:
                client = CosmosClient(self.host, self.master_key)
                container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                    self.TEST_CONTAINER_ID)
                for _ in range(5):
                    # Clear cache and immediately read
                    client.client_connection._routing_map_provider.clear_cache()
                    result = container.read_item(f"fi-{worker_id % 3}", partition_key=f"pk-{worker_id % 3}")
                    assert result["id"] == f"fi-{worker_id % 3}"
                client.close()
            except Exception as e:
                errors.append((worker_id, str(e)))

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(worker, i) for i in range(5)]
            for f in as_completed(futures):
                f.result()  # Re-raise exceptions

        self.assertEqual(len(errors), 0, f"Concurrent errors: {errors}")

    def test_pkrange_readonly_fields_not_corrupted(self):
        """PKRange namedtuple fields are immutable and cannot be accidentally modified."""
        pk = PKRange(id="0", minInclusive="", maxExclusive="FF", parents=[])

        # Namedtuple fields are read-only
        with self.assertRaises(AttributeError):
            pk.id = "modified"

        with self.assertRaises(AttributeError):
            pk.minInclusive = "modified"

        # Original values unchanged
        self.assertEqual(pk.id, "0")
        self.assertEqual(pk.maxExclusive, "FF")

        # Dict-style access still works
        self.assertEqual(pk["id"], "0")
        self.assertEqual(pk.get("minInclusive"), "")

    def test_transient_failure_during_cache_population(self):
        """SDK retries and eventually populates cache after a transient PKRange fetch failure."""
        transport = FaultInjectionTransport()
        transient_error = CosmosHttpResponseError(
            status_code=503,
            message="Service temporarily unavailable."
        )

        is_pkranges_call = lambda r: "pkranges" in r.url

        transport.add_fault(
            predicate=is_pkranges_call,
            fault_factory=lambda r: FaultInjectionTransport.error_after_delay(0, transient_error),
            max_inner_count=1,
        )

        client = self._make_fault_client(transport)
        try:
            container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)

            # First pkranges call fails (503), SDK retries, second succeeds
            result = container.read_item("fi-0", partition_key="pk-0")
            self.assertEqual(result["id"], "fi-0")

            # Cache should be populated
            cache = client.client_connection._routing_map_provider._collection_routing_map_by_item
            self.assertTrue(len(cache) > 0)
        finally:
            client.close()

    def test_clear_cache_during_concurrent_reads(self):
        """Clearing cache while reads are in progress doesn't cause crashes."""
        stop_event = threading.Event()
        errors = []

        def reader():
            client = CosmosClient(self.host, self.master_key)
            container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)
            try:
                while not stop_event.is_set():
                    try:
                        container.read_item("fi-0", partition_key="pk-0")
                    except Exception as e:
                        errors.append(str(e))
                        break
            finally:
                client.close()

        # Start readers
        threads = [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()

        # Rapidly clear cache while reads are happening
        for _ in range(10):
            self.client.client_connection._routing_map_provider.clear_cache()

        stop_event.set()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(len(errors), 0, f"Errors during concurrent reads: {errors}")


if __name__ == "__main__":
    unittest.main()
