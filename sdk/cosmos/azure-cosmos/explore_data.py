"""
Data exploration script to understand the structure of the data.
"""

import sys
sys.path.insert(0, r"c:\cosmos\python-sdk-repo\azure-sdk-for-python\sdk\cosmos\azure-cosmos")

from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

COSMOS_ENDPOINT = "https://tvk-my-cosmos-account.documents.azure.com:443/"
DATABASE_NAME = "spark-load-tests"
CONTAINER_NAME = "normal-bulk"

print("=" * 80)
print("Data Exploration")
print("=" * 80)

credential = DefaultAzureCredential()
client = CosmosClient(url=COSMOS_ENDPOINT, credential=credential)
container = client.get_database_client(DATABASE_NAME).get_container_client(CONTAINER_NAME)

# 1. Get a few sample items
print("\n1. Sample Items:")
items = list(container.query_items(
    query="SELECT TOP 3 * FROM c",
    enable_cross_partition_query=True
))
print(f"   Retrieved {len(items)} items")
if items:
    print(f"   Keys: {list(items[0].keys())}")
    print(f"   Sample item: {items[0]}")

# 2. Get partition key information
print("\n2. Partition Key Analysis:")
pk_query = "SELECT DISTINCT c.partitionKey FROM c"
partitions = list(container.query_items(
    query=pk_query,
    enable_cross_partition_query=True
))
print(f"   Total distinct partition keys: {len(partitions)}")
if partitions:
    print(f"   Sample partition keys: {[p['partitionKey'] for p in partitions[:5]]}")

# 3. Count items in specific partition
test_partition = "2022-05-07-02"
print(f"\n3. Items in partition '{test_partition}':")
partition_items = list(container.query_items(
    query=f"SELECT TOP 5 * FROM c WHERE c.partitionKey = '{test_partition}'"
))
print(f"   Retrieved {len(partition_items)} items")
if partition_items:
    print(f"   Sample: {partition_items[0]}")

# 4. Check data types
print("\n4. Data Structure:")
if items:
    for key, value in items[0].items():
        print(f"   {key}: {type(value).__name__} = {str(value)[:50]}")

print("\n" + "=" * 80)
