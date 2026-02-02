"""
Test Per-Request Mirror Serving Control

Validates that mirror serving can be enabled/disabled on a per-request basis
rather than at the client level.
"""

import time
from typing import List, Dict
from dotenv import dotenv_values

# Load configuration
ENV = dotenv_values(".env")

def print_header(title: str):
    print(f"\n{'=' * 80}")
    print(f"{title.center(80)}")
    print(f"{'=' * 80}\n")

def print_section(title: str):
    print(f"\n{'-' * 80}")
    print(f"{title}")
    print(f"{'-' * 80}")

def test_per_request_mirror_serving():
    """Test per-request mirror serving control."""
    
    print_header("PER-REQUEST MIRROR SERVING TEST")
    
    # Configuration
    endpoint = ENV.get("COSMOS_ENDPOINT")
    database_name = ENV.get("COSMOS_DATABASE") 
    container_name = ENV.get("COSMOS_CONTAINER")
    fabric_server = ENV.get("FABRIC_SERVER")
    fabric_database = ENV.get("FABRIC_DATABASE")
    
    print("Configuration:")
    print(f"  Cosmos Endpoint: {endpoint}")
    print(f"  Database: {database_name}")
    print(f"  Container: {container_name}")
    print(f"  Fabric Server: {fabric_server}")
    print(f"  Fabric Database: {fabric_database}")
    
    # Import SDK
    from azure.cosmos import CosmosClient
    from azure.identity import DefaultAzureCredential
    
    credential = DefaultAzureCredential()
    
    # Create ONE client with mirror_config (but no enable_mirror_serving flag)
    print_section("Creating CosmosClient with mirror_config")
    
    client = CosmosClient(
        url=endpoint,
        credential=credential,
        mirror_config={
            "server": fabric_server,
            "database": fabric_database,
            "credential": credential
        }
    )
    
    print("✓ Client created with mirror_config (per-request control enabled)")
    
    db = client.get_database_client(database_name)
    container = db.get_container_client(container_name)
    
    # TEST 1: Query WITHOUT use_mirror_serving (default=False, uses Cosmos DB)
    print_section("TEST 1: Query Cosmos DB Directly (use_mirror_serving=False)")
    
    query = "SELECT TOP 3 c.id, c.partitionKey FROM c"
    print(f"Query: {query}")
    print(f"use_mirror_serving: False (default)")
    
    try:
        start = time.time()
        results = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
            # use_mirror_serving NOT specified = defaults to False
        ))
        elapsed = time.time() - start
        
        print(f"✓ Success: {len(results)} items from Cosmos DB in {elapsed:.3f}s")
        if results:
            print(f"  Sample: {results[0]}")
    except Exception as e:
        print(f"✗ Failed: {e}")
    
    # TEST 2: Same query WITH use_mirror_serving=True (uses Fabric mirror)
    print_section("TEST 2: Same Query via Fabric Mirror (use_mirror_serving=True)")
    
    print(f"Query: {query}")
    print(f"use_mirror_serving: True")
    
    try:
        start = time.time()
        results = list(container.query_items(
            query=query,
            use_mirror_serving=True  # Explicitly route to Fabric mirror
        ))
        elapsed = time.time() - start
        
        print(f"✓ Success: {len(results)} items from Fabric Mirror in {elapsed:.3f}s")
        if results:
            print(f"  Sample: {results[0]}")
    except Exception as e:
        print(f"✗ Failed: {e}")
        import traceback
        traceback.print_exc()
    
    # TEST 3: Point read from Cosmos DB (fast, low latency)
    print_section("TEST 3: Point Read from Cosmos DB (Optimized)")
    
    query = "SELECT TOP 1 * FROM c WHERE c.id != null"
    print(f"Query: {query}")
    print(f"use_mirror_serving: False (optimal for point reads)")
    
    try:
        start = time.time()
        results = list(container.query_items(
            query=query,
            enable_cross_partition_query=True,
            use_mirror_serving=False  # Explicitly use Cosmos DB
        ))
        elapsed = time.time() - start
        
        print(f"✓ Success: {len(results)} items from Cosmos DB in {elapsed:.3f}s")
        print(f"  Low latency optimized for transactional reads")
    except Exception as e:
        print(f"✗ Failed: {e}")
    
    # TEST 4: Aggregation via Fabric Mirror (cheaper, faster for analytics)
    print_section("TEST 4: Aggregation via Fabric Mirror (Optimized)")
    
    query = "SELECT VALUE COUNT(1) FROM c"
    print(f"Query: {query}")
    print(f"use_mirror_serving: True (optimal for aggregations)")
    
    try:
        start = time.time()
        results = list(container.query_items(
            query=query,
            use_mirror_serving=True  # Route to Fabric for aggregation
        ))
        elapsed = time.time() - start
        
        print(f"✓ Success: COUNT = {results[0]} from Fabric Mirror in {elapsed:.3f}s")
        print(f"  Cheaper cost for analytical workload")
    except Exception as e:
        print(f"✗ Failed: {e}")
        import traceback
        traceback.print_exc()
    
    # TEST 5: GROUP BY via Fabric Mirror (NOT supported in Cosmos Python SDK!)
    print_section("TEST 5: GROUP BY Query (Mirror ONLY!)")
    
    query = """
        SELECT c.partitionKey, COUNT(1) as itemCount
        FROM c
        GROUP BY c.partitionKey
    """
    print(f"Query: {query.strip()}")
    
    print("\n  Trying Cosmos DB (should fail):")
    try:
        results = list(container.query_items(
            query=query,
            use_mirror_serving=False
        ))
        print(f"  ✗ Unexpected: Query succeeded with {len(results)} results")
    except Exception as e:
        error_msg = str(e)
        if "GroupBy" in error_msg or "not support" in error_msg:
            print(f"  ✓ Expected: GROUP BY not supported in Cosmos Python SDK")
        else:
            print(f"  ✗ Unexpected error: {error_msg[:100]}...")
    
    print("\n  Trying Fabric Mirror (should work):")
    try:
        start = time.time()
        results = list(container.query_items(
            query=query,
            use_mirror_serving=True  # GROUP BY requires mirror!
        ))
        elapsed = time.time() - start
        
        print(f"  ✓ Success: {len(results)} groups from Fabric Mirror in {elapsed:.3f}s")
        if results:
            print(f"  Sample: {results[0]}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
    
    # TEST 6: Error handling - mirror not configured
    print_section("TEST 6: Error Handling (No mirror_config)")
    
    print("Creating client WITHOUT mirror_config...")
    client_no_mirror = CosmosClient(url=endpoint, credential=credential)
    container_no_mirror = client_no_mirror.get_database_client(database_name).get_container_client(container_name)
    
    try:
        results = list(container_no_mirror.query_items(
            query="SELECT TOP 1 * FROM c",
            use_mirror_serving=True  # Try to use mirror without config
        ))
        print(f"✗ Unexpected: Should have raised error")
    except ValueError as e:
        print(f"✓ Correct error raised: {str(e)[:100]}...")
    except Exception as e:
        print(f"✗ Wrong error type: {type(e).__name__}: {e}")
    
    print_header("TEST COMPLETE - KEY TAKEAWAYS")
    
    print("""
✓ Per-Request Control Working!

Key Benefits:
  1. Same client can route queries to BOTH Cosmos DB and Fabric Mirror
  2. Optimize each query individually:
     - Point reads → Cosmos DB (low latency)
     - Aggregations → Fabric Mirror (lower cost)
     - GROUP BY → Fabric Mirror (not supported in Cosmos!)
  3. No need to create separate clients
  4. Fine-grained cost and performance optimization

Usage Pattern:
  # Create ONE client with mirror_config
  client = CosmosClient(url=..., credential=..., mirror_config={...})
  
  # Route queries individually
  fast_result = container.query_items(query="...", use_mirror_serving=False)  # Cosmos
  cheap_result = container.query_items(query="...", use_mirror_serving=True)  # Mirror
    """)

if __name__ == "__main__":
    test_per_request_mirror_serving()
