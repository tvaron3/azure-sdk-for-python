import os
import random
import sys

from azure.cosmos import CosmosClient

sys.path.append(r"")


import time
from datetime import datetime

import sys
import logging

# Replace with your Cosmos DB details
preferred_regions_2 = ['East US 2 EUAP', 'Central US EUAP']
COSMOS_URI2 = ""
COSMOS_KEY2 = ""


def get_random_item():
    random_int = random.randint(1, 10000)
    return {"id": "Simon-" + str(random_int), "pk": "pk-" + str(random_int)}


def upsert_item(container, num_upserts):
    for _ in range(num_upserts):
        container.upsert_item(get_random_item())


def read_item(container, num_upserts):
    for _ in range(num_upserts):
        item = get_random_item()
        container.read_item(item["id"], item["pk"])

def perform_query(container):
    random_item = get_random_item()
    results = container.query_items(query="SELECT * FROM c where c.id=@id and c.pk=@pk",
                                    parameters=[{"name": "@id", "value": random_item["id"]},
                                                {"name": "@pk", "value": random_item["pk"]}],
                                    partition_key=random_item["pk"])
    items = [item for item in results]


def query_items(cont, num_queries):
    for _ in range(num_queries):
        perform_query(cont)


def multi_region(client_id):
    with CosmosClient(COSMOS_URI2, COSMOS_KEY2, preferred_locations=preferred_regions_2,
                           enable_diagnostics_logging=True, logger=logger,
                           user_agent="Concurrent-VM-Sync" + str(client_id) + datetime.now().strftime(
                               "%Y%m%d-%H%M%S")) as client:
        db = client.get_database_client("SimonDB")
        cont = db.create_container_if_not_exists("SimonContainer", "/pk")
        time.sleep(1)

        while True:
            try:
                upsert_item(cont, 5)  # Number of concurrent upserts
                time.sleep(1)
                read_item(cont, 5)  # Number of concurrent reads
                time.sleep(1)
                query_items(cont, 2)  # Number of concurrent queries
                time.sleep(1)
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
    multi_region(file_name)
