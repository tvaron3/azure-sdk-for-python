import os.path
import random
import sys

sys.path.append(r"")

from azure.cosmos.aio import CosmosClient as AsyncClient
import asyncio

import time
from datetime import datetime

import sys
import logging

# Replace with your Cosmos DB details
preferred_regions_2 = ['East US 2 EUAP', 'Central US EUAP']
COSMOS_URI2 = ""
COSMOS_KEY2 = ""


async def write_item_concurrently(container, num_upserts, start_id):
    tasks = []
    for _ in range(num_upserts):
        tasks.append(container.upsert_item({"id": "Simon-" + str(start_id), "pk": "pk-" + str(start_id)}))
        start_id += 1
    await asyncio.gather(*tasks)


def get_random_item():
    random_int = random.randint(1, 10000)
    return {"id": "Simon-" + str(random_int), "pk": "pk-" + str(random_int)}


async def upsert_item_concurrently(container, num_upserts):
    tasks = []
    for _ in range(num_upserts):
        tasks.append(container.upsert_item(get_random_item()))
    await asyncio.gather(*tasks)


async def read_item_concurrently(container, num_upserts):
    tasks = []
    for _ in range(num_upserts):
        item = get_random_item()
        tasks.append(container.read_item(item["id"], item["pk"]))
    await asyncio.gather(*tasks)


async def query_items_concurrently(container, num_queries):
    tasks = []
    for _ in range(num_queries):
        tasks.append(perform_query(container))
    await asyncio.gather(*tasks)


async def perform_query(container):
    random_item = get_random_item()
    results = container.query_items(query="SELECT * FROM c", partition_key=random_item["pk"])
    items = [item async for item in results]


async def change_feed(container):
    response = container.query_items_change_feed(is_start_from_beginning=True)

    count = 0
    async for doc in response:
        count += 1


async def multi_region(client_id):
    async with AsyncClient(COSMOS_URI2, COSMOS_KEY2, preferred_locations=preferred_regions_2,
                           enable_diagnostics_logging=True, logger=logger,
                           user_agent="Concurrent-VM-" + str(client_id) + datetime.now().strftime(
                               "%Y%m%d-%H%M%S")) as client:
        db = client.get_database_client("SimonDB")
        cont = await db.create_container_if_not_exists("SimonContainer", "/pk")
        time.sleep(1)

        while True:
            try:
                await upsert_item_concurrently(cont, 5)  # Number of concurrent upserts
                time.sleep(1)
                await read_item_concurrently(cont, 5)  # Number of concurrent reads
                time.sleep(1)
                # await query_items_concurrently(cont, 500)  # Number of concurrent queries
                # time.sleep(1)
                # await change_feed(cont)
                # time.sleep(1)
            except Exception as e:
                raise e


if __name__ == "__main__":
    logger = logging.getLogger('azure.cosmos')
    file_name = os.path.basename(__file__)
    file_handler = logging.FileHandler(file_name + '-testing-' + datetime.now().strftime("%Y%m%d-%H%M%S") + '.log')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    asyncio.run(multi_region(file_name))
    # asyncio.run(create_item())
