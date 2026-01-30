"""
Test Mirror Integration - Direct Test Script

Tests the mirror serving integration directly in the SDK repo.
"""

import os
import sys
import time
from typing import Optional, Dict, Any, List

try:
    from dotenv import dotenv_values
    ENV = dotenv_values(".env")
except ImportError:
    print("Installing python-dotenv...")
    os.system("pip install python-dotenv")
    from dotenv import dotenv_values
    ENV = dotenv_values(".env")


def print_header(title: str):
    """Print a formatted header."""
    print(f"\n{'=' * 80}")
    print(f"{title.center(80)}")
    print(f"{'=' * 80}\n")


def print_section(title: str):
    """Print a section divider."""
    print(f"\n{'-' * 80}")
    print(f"{title}")
    print(f"{'-' * 80}")


def test_mirror_integration():
    """Test mirror integration with minimal dependencies."""
    
    print_header("MIRROR INTEGRATION TEST")
    
    # Load configuration
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
    
    if not all([endpoint, database_name, container_name, fabric_server, fabric_database]):
        print("\n❌ Missing required configuration in .env file!")
        sys.exit(1)
    
    # Import Cosmos SDK
    print_section("Importing Cosmos SDK")
    try:
        from azure.cosmos import CosmosClient
        from azure.identity import DefaultAzureCredential
        print("✓ Cosmos SDK imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import: {e}")
        sys.exit(1)
    
    # Check mirror integration availability
    print_section("Checking Mirror Integration")
    try:
        from azure.cosmos._mirror_integration import execute_mirrored_query, MirrorServingNotAvailableError
        print("✓ Mirror integration module found")
    except ImportError as e:
        print(f"❌ Mirror integration not available: {e}")
        sys.exit(1)
    
    # Create credential
    print_section("Creating Azure Credential")
    try:
        credential = DefaultAzureCredential()
        print("✓ DefaultAzureCredential created (using az login)")
    except Exception as e:
        print(f"❌ Failed to create credential: {e}")
        print("Make sure you've run 'az login'")
        sys.exit(1)
    
    # Test 1: Direct Cosmos client (no mirror)
    print_section("TEST 1: Direct Cosmos DB Client (No Mirror)")
    try:
        client_direct = CosmosClient(
            url=endpoint,
            credential=credential,
            enable_mirror_serving=False
        )
        print("✓ Direct client created")
        
        db = client_direct.get_database_client(database_name)
        container = db.get_container_client(container_name)
        
        # Simple query
        query = "SELECT TOP 3 c.id, c.partitionKey FROM c"
        print(f"\nExecuting query: {query}")
        
        start = time.time()
        results = list(container.query_items(query=query, enable_cross_partition_query=True))
        elapsed = time.time() - start
        
        print(f"✓ Query succeeded: {len(results)} items in {elapsed:.3f}s")
        if results:
            print(f"  Sample: {results[0]}")
        
    except Exception as e:
        print(f"❌ Direct query failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 2: Mirror-enabled client
    print_section("TEST 2: Mirror-Enabled Client")
    
    print("\nCreating mirror configuration...")
    print(f"  server: {fabric_server}")
    print(f"  database: {fabric_database}")
    print(f"  credential: <DefaultAzureCredential>")
    
    mirror_config = {
        "server": fabric_server,
        "database": fabric_database,
        "credential": credential
    }
    
    print("\nMirror config keys:", list(mirror_config.keys()))
    
    try:
        client_mirror = CosmosClient(
            url=endpoint,
            credential=credential,
            enable_mirror_serving=True,
            mirror_config=mirror_config
        )
        print("✓ Mirror-enabled client created")
        
        db = client_mirror.get_database_client(database_name)
        container = db.get_container_client(container_name)
        
        # Simple query via mirror
        query = "SELECT TOP 3 c.id, c.partitionKey FROM c"
        print(f"\nExecuting query via mirror: {query}")
        print(f"Container ID being used: {container.id}")
        
        start = time.time()
        results = list(container.query_items(query=query, enable_cross_partition_query=True))
        elapsed = time.time() - start
        
        print(f"✓ Mirror query succeeded: {len(results)} items in {elapsed:.3f}s")
        if results:
            print(f"  Sample: {results[0]}")
        
    except Exception as e:
        print(f"❌ Mirror query failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        
        # Try to diagnose
        print("\n🔍 Diagnostic Information:")
        print(f"   Mirror config provided: {mirror_config}")
        print(f"   Container name: {container_name}")
        
    # Test 3: GROUP BY query (should fail on Cosmos, work on Mirror)
    print_section("TEST 3: GROUP BY Query (Mirror Only!)")
    
    query = """
        SELECT c.partitionKey, COUNT(1) as itemCount 
        FROM c 
        GROUP BY c.partitionKey
    """
    
    print(f"Query: {query.strip()}")
    
    # Try on direct client
    print("\n🔷 Testing on Direct Cosmos DB:")
    try:
        client_direct = CosmosClient(url=endpoint, credential=credential, enable_mirror_serving=False)
        db = client_direct.get_database_client(database_name)
        container = db.get_container_client(container_name)
        
        results = list(container.query_items(query=query))
        print(f"❌ Unexpected: Query succeeded with {len(results)} results")
    except Exception as e:
        error_msg = str(e)
        if "GroupBy" in error_msg or "not support" in error_msg:
            print(f"✓ Expected failure: GROUP BY not supported in Cosmos Python SDK")
            print(f"   Error: {error_msg[:100]}...")
        else:
            print(f"❌ Unexpected error: {e}")
    
    # Try on mirror client
    print("\n🔶 Testing on Fabric Mirror:")
    try:
        client_mirror = CosmosClient(
            url=endpoint,
            credential=credential,
            enable_mirror_serving=True,
            mirror_config=mirror_config
        )
        db = client_mirror.get_database_client(database_name)
        container = db.get_container_client(container_name)
        
        start = time.time()
        results = list(container.query_items(query=query))
        elapsed = time.time() - start
        
        print(f"✓ Mirror query succeeded: {len(results)} groups in {elapsed:.3f}s")
        if results:
            print(f"  Sample: {results[0]}")
    except Exception as e:
        print(f"❌ Mirror query failed: {e}")
        import traceback
        traceback.print_exc()
    
    print_header("TEST COMPLETE")


if __name__ == "__main__":
    test_mirror_integration()
