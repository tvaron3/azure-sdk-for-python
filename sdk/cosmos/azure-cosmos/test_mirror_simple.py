"""Test mirror serving with real endpoints."""

# Don't manipulate path - use installed package
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

print("=" * 60)
print("Testing Mirror Serving Integration")
print("=" * 60)

credential = DefaultAzureCredential()

# Test 1: Normal Cosmos query
print("\n1. Normal Cosmos query...")
client1 = CosmosClient(
    url="https://tvk-my-cosmos-account.documents.azure.com:443/",
    credential=credential
)
container1 = client1.get_database_client("spark-load-tests").get_container_client("normal-bulk")
items1 = list(container1.query_items(query="SELECT TOP 2 * FROM c", enable_cross_partition_query=True))
print(f"   ✓ Got {len(items1)} items from Cosmos DB")

# Test 2: Mirror query
print("\n2. Mirror query via Fabric...")
try:
    client2 = CosmosClient(
        url="https://tvk-my-cosmos-account.documents.azure.com:443/",
        credential=credential,
        enable_mirror_serving=True,
        mirror_config={
            "fabric_server": "x6eps4xrq2xudenlfv6naeo3i4-go4uaawrmy3ulgkq7byxgxj3uy.msit-datawarehouse.fabric.microsoft.com",
            "fabric_database": "spark-load-tests",
            "fabric_table": "normal-bulk",
            "fabric_schema": "dbo"
        }
    )
    container2 = client2.get_database_client("spark-load-tests").get_container_client("normal-bulk")
    items2 = list(container2.query_items(query="SELECT TOP 2 * FROM c", enable_cross_partition_query=True))
    print(f"   ✓ Got {len(items2)} items from Fabric mirror")
    if items2:
        print(f"   ✓ Sample keys: {list(items2[0].keys())[:5]}")
    print("\n" + "=" * 60)
    print("✓✓✓ MIRROR SERVING IS WORKING! ✓✓✓")
    print("=" * 60)
except Exception as e:
    print(f"   ✗ Failed: {e}")
    import traceback
    traceback.print_exc()
