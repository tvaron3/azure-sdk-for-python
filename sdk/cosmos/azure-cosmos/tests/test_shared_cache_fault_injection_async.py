# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

"""Async fault injection tests for the shared partition key range cache.

Async counterparts of test_shared_cache_fault_injection.py, validating
cache refresh, concurrent access, and PKRange integrity under async I/O.
"""

import asyncio
import unittest

import pytest
import pytest_asyncio

import test_config
from _fault_injection_transport_async import FaultInjectionTransportAsync
from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey
from azure.cosmos._routing.routing_range import PKRange
from azure.cosmos._routing.aio.routing_map_provider import (
    _shared_routing_map_cache,
    _shared_cache_lock,
)
from azure.cosmos.exceptions import CosmosHttpResponseError


@pytest.mark.cosmosEmulator
@pytest.mark.asyncio
class TestSharedCacheFaultInjectionAsync(unittest.IsolatedAsyncioTestCase):
    """Async fault injection tests requiring the Cosmos emulator."""

    host = test_config.TestConfig.host
    master_key = test_config.TestConfig.masterKey
    TEST_DATABASE_ID = test_config.TestConfig.TEST_DATABASE_ID
    TEST_CONTAINER_ID = test_config.TestConfig.TEST_MULTI_PARTITION_CONTAINER_ID

    async def asyncSetUp(self):
        self.client = CosmosClient(self.host, self.master_key)
        db = self.client.get_database_client(self.TEST_DATABASE_ID)
        self.container = db.get_container_client(test_config.TestConfig.TEST_MULTI_PARTITION_CONTAINER_ID)
        for i in range(10):
            await self.container.upsert_item({"id": f"afi-{i}", "pk": f"pk-{i % 3}", "value": i})

    async def asyncTearDown(self):
        with _shared_cache_lock:
            _shared_routing_map_cache.clear()
        await self.client.close()

    async def test_gone_410_triggers_cache_refresh_async(self):
        """Async: 410 Gone triggers cache refresh and retry succeeds."""
        transport = FaultInjectionTransportAsync()
        gone_error = CosmosHttpResponseError(
            status_code=410,
            message="Partition has moved.",
            sub_status=1002
        )

        is_document_read = lambda r: (
            FaultInjectionTransportAsync.predicate_is_document_operation(r)
            and r.method == "GET"
        )
        transport.add_fault(
            predicate=is_document_read,
            fault_factory=lambda r: asyncio.create_task(FaultInjectionTransportAsync.error_after_delay(0, gone_error)),
            max_inner_count=1,
        )

        async with CosmosClient(self.host, self.master_key, transport=transport) as client:
            container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)
            result = await container.read_item("afi-0", partition_key="pk-0")
            self.assertEqual(result["id"], "afi-0")

    async def test_stale_cache_after_split_async(self):
        """Async: 410/1002 triggers refresh; second client sees updated cache."""
        transport = FaultInjectionTransportAsync()
        split_error = CosmosHttpResponseError(
            status_code=410,
            message="Partition key range is gone.",
            sub_status=1002
        )

        is_document_read = lambda r: (
            FaultInjectionTransportAsync.predicate_is_document_operation(r)
            and r.method == "GET"
        )
        transport.add_fault(
            predicate=is_document_read,
            fault_factory=lambda r: asyncio.create_task(FaultInjectionTransportAsync.error_after_delay(0, split_error)),
            max_inner_count=1,
        )

        async with CosmosClient(self.host, self.master_key, transport=transport) as client1:
            container1 = client1.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)
            result = await container1.read_item("afi-1", partition_key="pk-1")
            self.assertEqual(result["id"], "afi-1")

        async with CosmosClient(self.host, self.master_key) as client2:
            container2 = client2.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)
            result2 = await container2.read_item("afi-2", partition_key="pk-2")
            self.assertEqual(result2["id"], "afi-2")

    async def test_concurrent_cache_refresh_async(self):
        """Async: Multiple coroutines clearing cache + reading don't crash."""
        errors = []

        async def worker(worker_id):
            try:
                async with CosmosClient(self.host, self.master_key) as client:
                    container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                        self.TEST_CONTAINER_ID)
                    for _ in range(5):
                        client.client_connection._routing_map_provider.clear_cache()
                        result = await container.read_item(
                            f"afi-{worker_id % 3}", partition_key=f"pk-{worker_id % 3}")
                        assert result["id"] == f"afi-{worker_id % 3}"
            except Exception as e:
                errors.append((worker_id, str(e)))

        await asyncio.gather(*[worker(i) for i in range(5)])
        self.assertEqual(len(errors), 0, f"Async concurrent errors: {errors}")

    async def test_transient_failure_during_cache_population_async(self):
        """Async: SDK retries after transient PKRange fetch failure."""
        transport = FaultInjectionTransportAsync()
        transient_error = CosmosHttpResponseError(
            status_code=503,
            message="Service temporarily unavailable."
        )

        is_pkranges_call = lambda r: "pkranges" in r.url
        transport.add_fault(
            predicate=is_pkranges_call,
            fault_factory=lambda r: asyncio.create_task(FaultInjectionTransportAsync.error_after_delay(0, transient_error)),
            max_inner_count=1,
        )

        async with CosmosClient(self.host, self.master_key, transport=transport) as client:
            container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                self.TEST_CONTAINER_ID)
            result = await container.read_item("afi-0", partition_key="pk-0")
            self.assertEqual(result["id"], "afi-0")

            cache = client.client_connection._routing_map_provider._collection_routing_map_by_item
            self.assertTrue(len(cache) > 0)

    async def test_clear_cache_during_concurrent_reads_async(self):
        """Async: Clearing cache while reads are in-flight doesn't corrupt state."""
        stop_event = asyncio.Event()
        errors = []

        async def reader():
            async with CosmosClient(self.host, self.master_key) as client:
                container = client.get_database_client(self.TEST_DATABASE_ID).get_container_client(
                    self.TEST_CONTAINER_ID)
                while not stop_event.is_set():
                    try:
                        await container.read_item("afi-0", partition_key="pk-0")
                    except Exception as e:
                        errors.append(str(e))
                        break

        tasks = [asyncio.create_task(reader()) for _ in range(3)]

        # Rapidly clear cache
        for _ in range(10):
            self.client.client_connection._routing_map_provider.clear_cache()
            await asyncio.sleep(0.01)

        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(len(errors), 0, f"Errors during concurrent async reads: {errors}")

    async def test_pkrange_immutability_async(self):
        """Async: PKRange fields are immutable (namedtuple guarantee)."""
        pk = PKRange(id="0", minInclusive="", maxExclusive="FF", parents=[])
        with self.assertRaises(AttributeError):
            pk.id = "modified"
        self.assertEqual(pk["id"], "0")
        self.assertEqual(pk.get("maxExclusive"), "FF")


if __name__ == "__main__":
    unittest.main()
