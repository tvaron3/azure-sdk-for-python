"""
Test script to verify the Cosmos SDK integration with azure-cosmos-fabric-mapper.

This script tests:
1. Mirror serving disabled (default behavior)
2. Mirror serving enabled but mapper not installed
3. Mirror serving enabled with mapper installed (basic import test)
"""

import sys
import os

# Add the cosmos module to the path
cosmos_sdk_path = r"c:\cosmos\python-sdk-repo\azure-sdk-for-python\sdk\cosmos\azure-cosmos"
sys.path.insert(0, cosmos_sdk_path)

print("=" * 80)
print("Testing Cosmos SDK Mirror Serving Integration")
print("=" * 80)

# Test 1: Import the module
print("\n[Test 1] Importing azure.cosmos...")
try:
    from azure.cosmos import CosmosClient
    from azure.cosmos._mirror_integration import MirrorServingNotAvailableError
    print("✓ Successfully imported CosmosClient and MirrorServingNotAvailableError")
except ImportError as e:
    print(f"✗ Failed to import: {e}")
    sys.exit(1)

# Test 2: Create client without mirror serving (default)
print("\n[Test 2] Creating CosmosClient without mirror serving (default)...")
try:
    # Use a valid base64-encoded fake key (64 bytes)
    fake_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    client = CosmosClient(
        url="https://test-account.documents.azure.com:443/",
        credential=fake_key
    )
    assert hasattr(client.client_connection, '_enable_mirror_serving'), \
        "Missing _enable_mirror_serving attribute"
    assert client.client_connection._enable_mirror_serving is False, \
        "Mirror serving should be disabled by default"
    assert client.client_connection._mirror_config is None, \
        "Mirror config should be None by default"
    print("✓ Client created successfully with mirror serving disabled")
except Exception as e:
    print(f"✗ Failed to create client: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Create client with mirror serving enabled
print("\n[Test 3] Creating CosmosClient with mirror serving enabled...")
try:
    fake_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    client_with_mirror = CosmosClient(
        url="https://test-account.documents.azure.com:443/",
        credential=fake_key,
        enable_mirror_serving=True,
        mirror_config={
            "fabric_server": "test-fabric.msit-datawarehouse.fabric.microsoft.com",
            "fabric_database": "testdb",
            "fabric_table": "testtable",
            "fabric_schema": "dbo"
        }
    )
    assert client_with_mirror.client_connection._enable_mirror_serving is True, \
        "Mirror serving should be enabled"
    assert client_with_mirror.client_connection._mirror_config is not None, \
        "Mirror config should be set"
    assert client_with_mirror.client_connection._mirror_config["fabric_server"] == \
        "test-fabric.msit-datawarehouse.fabric.microsoft.com", \
        "Fabric server not set correctly"
    print("✓ Client created successfully with mirror serving enabled")
    print(f"  - fabric_server: {client_with_mirror.client_connection._mirror_config['fabric_server']}")
    print(f"  - fabric_database: {client_with_mirror.client_connection._mirror_config['fabric_database']}")
    print(f"  - fabric_table: {client_with_mirror.client_connection._mirror_config['fabric_table']}")
except Exception as e:
    print(f"✗ Failed to create client with mirror serving: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 4: Test environment variable support
print("\n[Test 4] Testing COSMOS_ENABLE_MIRROR_SERVING environment variable...")
try:
    fake_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    os.environ['COSMOS_ENABLE_MIRROR_SERVING'] = 'true'
    client_env = CosmosClient(
        url="https://test-account.documents.azure.com:443/",
        credential=fake_key,
        mirror_config={
            "fabric_server": "env-test.fabric.com",
            "fabric_database": "envdb",
            "fabric_table": "envtable"
        }
    )
    assert client_env.client_connection._enable_mirror_serving is True, \
        "Mirror serving should be enabled via environment variable"
    print("✓ Environment variable COSMOS_ENABLE_MIRROR_SERVING works correctly")
    os.environ.pop('COSMOS_ENABLE_MIRROR_SERVING')
except Exception as e:
    print(f"✗ Failed environment variable test: {e}")
    os.environ.pop('COSMOS_ENABLE_MIRROR_SERVING', None)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Check if mapper package is available
print("\n[Test 5] Checking if azure-cosmos-fabric-mapper is installed...")
try:
    from azure_cosmos_fabric_mapper import MirrorServingConfiguration
    from azure_cosmos_fabric_mapper.sdk_hook import contract
    print("✓ azure-cosmos-fabric-mapper is installed and importable")
    print(f"  - MirroredQueryRequest available: {hasattr(contract, 'MirroredQueryRequest')}")
    print(f"  - run_mirrored_query available: {hasattr(contract, 'run_mirrored_query')}")
    mapper_installed = True
except ImportError as e:
    print(f"⚠ azure-cosmos-fabric-mapper is not installed: {e}")
    print("  This is expected if you haven't installed it yet.")
    print("  To install: pip install azure-cosmos-fabric-mapper[odbc]")
    mapper_installed = False

# Test 6: Test lazy import error handling
print("\n[Test 6] Testing lazy import error handling...")
if not mapper_installed:
    print("Testing MirrorServingNotAvailableError...")
    try:
        from azure.cosmos._mirror_integration import _lazy_import_mapper
        _lazy_import_mapper()
        print("✗ Expected MirrorServingNotAvailableError but got none")
    except MirrorServingNotAvailableError as e:
        print("✓ MirrorServingNotAvailableError raised correctly")
        print(f"  Error message preview: {str(e)[:100]}...")
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
else:
    print("⚠ Skipping error test since mapper is installed")

# Summary
print("\n" + "=" * 80)
print("INTEGRATION TEST SUMMARY")
print("=" * 80)
print("✓ All core integration tests passed!")
print(f"✓ Mirror serving configuration: Working")
print(f"✓ Environment variable support: Working")
print(f"{'✓' if mapper_installed else '⚠'} Mapper package: {'Installed' if mapper_installed else 'Not installed (install to test full functionality)'}")
print("\nNext steps:")
if not mapper_installed:
    print("1. Install azure-cosmos-fabric-mapper package:")
    print("   cd C:\\cosmos\\fabric-abstraction\\azure-cosmos-fabric-mapper")
    print("   pip install -e .[odbc]")
    print("2. Run this test again to verify end-to-end integration")
else:
    print("1. Test actual query execution with a real Fabric endpoint")
    print("2. Verify query translation and result mapping")
print("=" * 80)
