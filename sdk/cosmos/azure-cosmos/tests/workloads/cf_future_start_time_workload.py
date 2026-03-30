# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Writes a few documents, then queries the change feed with a start_time
# 1 hour in the future and prints the resulting continuation token.

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from azure.cosmos import documents
from azure.cosmos.aio import CosmosClient as AsyncClient
from workload_configs import *
from workload_utils import create_logger, create_random_item


async def run():
    connection_policy = documents.ConnectionPolicy()
    connection_policy.UseMultipleWriteLocations = USE_MULTIPLE_WRITABLE_LOCATIONS

    async with AsyncClient(
        COSMOS_URI,
        COSMOS_CREDENTIAL,
        connection_policy=connection_policy,
        preferred_locations=PREFERRED_LOCATIONS,
    ) as client:
        db = client.get_database_client(COSMOS_DATABASE)
        container = db.get_container_client(COSMOS_CONTAINER)

        # Write a few documents
        print("Creating 5 documents...")
        for _ in range(5):
            item = create_random_item()
            await container.upsert_item(item)
            print(f"  Created item {item['id']}")

        # Query change feed with start_time 1 hour in the future
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        print(f"\nQuerying change feed with start_time = {future_time.isoformat()}")

        response_iterator = container.query_items_change_feed(start_time=future_time)
        items = [item async for item in response_iterator]
        print(f"Items returned: {len(items)}")

        continuation_token = container.client_connection.last_response_headers["etag"]
        print(f"\nContinuation token:\n{continuation_token}")


if __name__ == "__main__":
    _, logger = create_logger("cf_future_start_time_workload.py")
    asyncio.run(run())
