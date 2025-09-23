#Final with two hosts without prefetch
import logging
import os
import uuid
import asyncio
import unittest
from logging.handlers import RotatingFileHandler

import pytest

from azure.identity import InteractiveBrowserCredential
from azure.cosmos import exceptions
from azure.cosmos.aio import CosmosClient
from workloads.workload_utils import get_user_agent, WorkloadLoggerFilter


def get_test_item(i: int, pk: str, unique_id: str):
    return {
        "id": f"Item_{unique_id}_{i}",
        "pk": pk,
        "test_object": True,
        "lastName": "Smith",
        "value": f"sample-{i}"
    }


async def _create_one(host: str, database_id: str, container_id: str, pk: str, i: int, credential) -> str:
    client = CosmosClient(host, credential)
    try:
        db = client.get_database_client(database_id)
        container = db.get_container_client(container_id)
        uid = str(uuid.uuid4())
        item = get_test_item(i, pk, uid)
        created = await container.create_item(item)
        return created["id"]
    finally:
        await client.close()


def create_logger(file_name):
    logger = logging.getLogger()
    prefix = os.path.splitext(file_name)[0] + "-" + str(os.getpid())
    # Create a rotating file handler
    handler = RotatingFileHandler(
        "log-" + get_user_agent(prefix) + '.log',
        maxBytes=1024 * 1024 * 10,  # 10 mb
        backupCount=2
    )
    logger.setLevel(logging.DEBUG)
    # create filters for the logger handler to reduce the noise
    workload_logger_filter = WorkloadLoggerFilter()
    handler.addFilter(workload_logger_filter)
    logger.addHandler(handler)
    return prefix, logger




@pytest.mark.cosmosEmulator
class TestAADAsync(unittest.IsolatedAsyncioTestCase):
    async def test_aad_scope_override_async(self):
        os.environ["AZURE_COSMOS_AAD_SCOPE_OVERRIDE"] = "https://cosmos.azure.com/.default"

        host1 = "https://tomasvaron-cdb.documents.azure.com:443/"
        host2 = "https://efbe0de8-a4fe-4e2c-abf1-1fc8fbb8dd41.zef.msit-sql.cosmos.fabric.microsoft.com:443/"
        database_id = "tomasvaron-test"
        container_id = "scale_cont"
        partition_key_value = "partition1"

        credential = InteractiveBrowserCredential()
        await asyncio.to_thread(credential.get_token, "https://cosmos.azure.com/.default")

        try:
            tasks = [
                _create_one(host1 if i < 5 else host2, database_id, container_id, partition_key_value, i, credential)
                for i in range(10)
            ]
            created_ids = await asyncio.gather(*tasks)
            assert len(created_ids) == 10
        except exceptions.CosmosHttpResponseError as ex:
            self.fail(f"CosmosHttpResponseError occurred: {ex.message}")



if __name__ == "__main__":
    unittest.main()