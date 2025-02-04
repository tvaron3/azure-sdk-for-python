import os
import random
import sys

sys.path.append(r"")

import uuid
from azure.cosmos.aio import CosmosClient as AsyncClient
import asyncio

import time
from datetime import datetime

import sys
import logging

# Create a logger for the "azure" SDK
# logger = logging.getLogger("azure")
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler(stream=sys.stdout)
# logger.addHandler(handler)

# Replace with your Cosmos DB details
preferred_regions_2 = ['East US 2 EUAP', 'Central US EUAP']
COSMOS_URI2 = ""
COSMOS_KEY2 = ""


async def write_item_concurrently_initial(container, num_upserts, initial):
    tasks = []
    for i in range(initial, initial + num_upserts):
        tasks.append(container.upsert_item({"id": "Simon-" + str(i), "pk": "pk-" + str(i)}))
    await asyncio.gather(*tasks)


async def write_item_concurrently(container, num_upserts):
    tasks = []
    for _ in range(num_upserts):
        tasks.append(
            container.upsert_item({"id": "Simon-" + str(uuid.uuid4()), "pk": "pk-" + str(random.randint(1, 10000))}))
    await asyncio.gather(*tasks)


def get_random_item():
    random_int = random.randint(1, 10000)
    return {"id": "Simon-" + str(random_int), "pk": "pk-" + str(random_int)}


def get_upsert_random_item():
    random_int = random.randint(1, 1000000000)
    return {"id": "Simon-" + str(random_int), "pk": "pk-" + str(random_int)}


async def upsert_item_concurrently(container, num_upserts):
    tasks = []
    for _ in range(num_upserts):
        tasks.append(container.upsert_item(get_upsert_random_item()))
    await asyncio.gather(*tasks)


async def read_item_concurrently(container, num_upserts):
    tasks = []
    for _ in range(num_upserts):
        item = get_random_item()
        print(item["id"], item["pk"])
        tasks.append(container.read_item(item["id"], item["pk"]))
    await asyncio.gather(*tasks)


async def multi_region():
    logger = logging.getLogger('azure.cosmos')
    file_handler = logging.FileHandler('fiddler_testing_2.txt')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    async with AsyncClient(COSMOS_URI2, COSMOS_KEY2, preferred_locations=preferred_regions_2,
                           enable_diagnostics_logging=True, logger=logger,
                           user_agent="Concurrent-Tomas") as client:
        db = client.get_database_client("SimonDB")
        cont = await db.create_container_if_not_exists("SimonContainer", "/pk")
        time.sleep(1)

        while True:
            try:
                await upsert_item_concurrently(cont, 100)  # Number of concurrent upserts
                time.sleep(1)
                await read_item_concurrently(cont, 100)  # Number of concurrent reads
            except Exception as e:
                raise e


async def create_items(client_id):
    async with AsyncClient(COSMOS_URI2, COSMOS_KEY2, preferred_locations=preferred_regions_2,
                           enable_diagnostics_logging=True, logger=logger,
                           user_agent="Concurrent-Write-Tomas" + str(client_id) + str(
                               client_id) + datetime.now().strftime("%Y%m%d-%H%M%S")) as client:
        db = await client.create_database_if_not_exists("SimonDB")
        cont = await db.create_container_if_not_exists("SimonContainer", "/pk")
        time.sleep(1)

        try:
            while True:
                await write_item_concurrently(cont, 4)  # Number of concurrent upserts
                time.sleep(1)
        except Exception as e:
            raise e


async def create_items_initial():
    async with AsyncClient(COSMOS_URI2, COSMOS_KEY2, preferred_locations=preferred_regions_2,
                           enable_diagnostics_logging=True, logger=logger,
                           user_agent="Concurrent-Write-VM-" + datetime.now().strftime("%Y%m%d-%H%M%S")) as client:
        db = await client.create_database_if_not_exists("SimonDB")
        cont = await db.create_container_if_not_exists("SimonContainer", "/pk")
        time.sleep(1)

        try:
            for i in range(0, 10000, 1000):
                await write_item_concurrently_initial(cont, 1000, i)  # Number of concurrent upserts
                time.sleep(1)
        except Exception as e:
            raise e


async def create_item():
    logger = logging.getLogger('azure.cosmos')
    file_handler = logging.FileHandler('create_item_testing.log')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    async with AsyncClient(COSMOS_URI2, COSMOS_KEY2, preferred_locations=preferred_regions_2,
                           enable_diagnostics_logging=True, logger=logger,
                           user_agent="-Concurrent-Write-Tomas") as client:
        db = await client.create_database_if_not_exists("SimonDB")
        cont = await db.create_container_if_not_exists("SimonContainer", "/pk")
        time.sleep(1)

        try:
            # await cont.create_item({"id": "Simon-4082", "pk": "pk-4082"})
            await cont.read_item("Simon-4082", "pk-4082")
        except Exception as e:
            raise e


if __name__ == "__main__":
    logger = logging.getLogger('azure.cosmos')
    file_name = os.path.basename(__file__)
    file_handler = logging.FileHandler(file_name + '-testing-' + datetime.now().strftime("%Y%m%d-%H%M%S") + '.log')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    asyncio.run(create_items(file_name))
    # asyncio.run(create_items_initial())
