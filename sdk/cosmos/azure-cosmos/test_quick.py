"""
Quick test for mirror serving with real endpoints.
"""

import sys
sys.path.insert(0, r"c:\cosmos\python-sdk-repo\azure-sdk-for-python\sdk\cosmos\azure-cosmos")

from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

print("Testing mirror serving integration...")

# Config
COSMOS_ENDPOINT = "https://tvk-my-cosmos-account.documents.azure.com:443/"
DATABASE_NAME = "spark-load-tests"
CONTAINER_NAME = "normal-bulk"

FABRIC_SERVER = "x6eps4xrq2xudenlfv6naeo3i4-go4uaawrmy3ulgkq7byxgxj3uy.msit-datawarehouse.fabric.microsoft.com"
FABRIC_DATABASE = "spark-load-tests"
FABRIC_TABLE = "normal-bulk"

credential = DefaultAzureCredential()

# Test 1: Normal query
print("\n1. Querying Cosmos DB normally...")
client1 = CosmosClient(url=COSMOS_ENDPOINT, credential=credential)
container1 = client1.get_database_client(DATABASE_NAME).get_container_client(CONTAINER_NAME)
items1 = list(container1.query_items(query="SELECT TOP 3 * FROM c", enable_cross_partition_query=True))
print(f"   Got {len(items1)} items from Cosmos DB")

# Test 2: Mirror query
print("\n2. Querying via Fabric mirror...")
try:
    client2 = CosmosClient(
        url=COSMOS_ENDPOINT,
        credential=credential,
        enable_mirror_serving=True,
        mirror_config={
            "fabric_server": FABRIC_SERVER,
            "fabric_database": FABRIC_DATABASE,
            "fabric_table": FABRIC_TABLE,
            "fabric_schema": "dbo"
        }
    )
    container2 = client2.get_database_client(DATABASE_NAME).get_container_client(CONTAINER_NAME)
    items2 = list(container2.query_items(query="SELECT TOP 3 * FROM c", enable_cross_partition_query=True))
    print(f"   Got {len(items2)} items from Fabric mirror")
    print(f"   Sample: {str(items2[0])[:150]}...")
    print("\n✓ Mirror serving is working!")
except Exception as e:
    print(f"\n✗ Mirror query failed: {e}")
    import traceback
    traceback.print_exc()
