# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.
import sys

from azure.cosmos import documents
from workload_utils import *
from workload_configs import *
sys.path.append(r"/")

from azure.cosmos.aio import CosmosClient as AsyncClient
import asyncio

async def run_workload(client_id, client_logger, uri):
    os.environ["AZURE_COSMOS_AAD_SCOPE_OVERRIDE"] = "https://cosmos.azure.com/.default"
    # token = await asyncio.to_thread(COSMOS_CREDENTIAL.get_token, "https://cosmos.azure.com/.default")

    connectionPolicy = documents.ConnectionPolicy()
    connectionPolicy.UseMultipleWriteLocations = USE_MULTIPLE_WRITABLE_LOCATIONS
    async with AsyncClient(uri, COSMOS_CREDENTIAL, connection_policy=connectionPolicy,
                           preferred_locations=PREFERRED_LOCATIONS, excluded_locations=CLIENT_EXCLUDED_LOCATIONS,
                           enable_diagnostics_logging=True, logger=client_logger,
                           user_agent=get_user_agent(client_id)) as client:
        db = client.get_database_client(COSMOS_DATABASE)
        cont = db.get_container_client(COSMOS_CONTAINER)
        await asyncio.sleep(1)

        try:
            await upsert_item_concurrently(cont, REQUEST_EXCLUDED_LOCATIONS, CONCURRENT_REQUESTS)
            await read_item_concurrently(cont, REQUEST_EXCLUDED_LOCATIONS, CONCURRENT_REQUESTS)
            await query_items_concurrently(cont, REQUEST_EXCLUDED_LOCATIONS, CONCURRENT_QUERIES)
        except Exception as e:
            client_logger.info("Exception in application layer")
            client_logger.error(e)

# Added helper to run multiple clients concurrently
async def run_multiple_clients(num_clients: int, prefix: str, logger):

    tasks = []
    for i in range(num_clients):
        uri = None
        if i % 2 == 0:
            uri = COSMOS_URI
        else:
            uri = COSMOS_URI_2
        client_id = f"{prefix}-client-{i}"
        tasks.append(asyncio.create_task(run_workload(client_id, logger, uri)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    file_name = os.path.basename(__file__)
    prefix, logger = create_logger(file_name)
    # Run 10 clients concurrently
    asyncio.run(run_multiple_clients(10, prefix, logger))
