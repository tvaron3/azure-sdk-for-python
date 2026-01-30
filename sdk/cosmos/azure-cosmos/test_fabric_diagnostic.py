"""
Diagnostic test to check Fabric connectivity and table access.
"""

from azure.identity import DefaultAzureCredential
from azure_cosmos_fabric_mapper import MirrorServingConfiguration
from azure_cosmos_fabric_mapper.credentials import DefaultAzureSqlCredential
from azure_cosmos_fabric_mapper.driver import get_driver_client

print("=" * 80)
print("Fabric Connectivity Diagnostic")
print("=" * 80)

# Configuration
config = MirrorServingConfiguration(
    fabric_server="x6eps4xrq2xudenlfv6naeo3i4-go4uaawrmy3ulgkq7byxgxj3uy.msit-datawarehouse.fabric.microsoft.com",
    fabric_database="spark-load-tests",
    fabric_table="normal-bulk",
    fabric_schema="dbo"
)

print("\nConfiguration:")
print(f"  Server: {config.fabric_server}")
print(f"  Database: {config.fabric_database}")
print(f"  Table: {config.fabric_table}")
print(f"  Schema: {config.fabric_schema}")

# Create credentials and driver
print("\nCreating credentials and driver...")
credentials = DefaultAzureSqlCredential()
driver = get_driver_client(config=config, credentials=credentials)
print(f"  Driver type: {type(driver).__name__}")

# Try a simple query
print("\nExecuting test query...")
try:
    # Try to query the information schema first to see what tables exist
    sql = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%bulk%'"
    print(f"  SQL: {sql}")
    result = driver.execute(sql, [])
    rows = list(result.rows)
    print(f"  Found {len(rows)} matching tables:")
    for row in rows:
        print(f"    - {row}")
except Exception as e:
    print(f"  ✗ Query failed: {e}")
    import traceback
    traceback.print_exc()

# Try the actual table
print("\nTrying to access the configured table...")
try:
    # Use the database name as schema (Fabric Warehouse pattern)
    sql = f"SELECT TOP 1 * FROM [{config.fabric_database}].[{config.fabric_table}]"
    print(f"  SQL: {sql}")
    result = driver.execute(sql, [])
    rows = list(result.rows)
    print(f"  ✓ Successfully accessed table!")
    print(f"  Columns: {result.columns}")
    if rows:
        print(f"  Sample row: {rows[0]}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    
    # Try with different quoting
    print("\n  Trying alternate table naming...")
    try:
        sql = f"SELECT TOP 1 * FROM [{config.fabric_schema}].[normal_bulk]"
        print(f"    SQL: {sql}")
        result = driver.execute(sql, [])
        rows = list(result.rows)
        print(f"    ✓ Table exists as 'normal_bulk' (underscore)!")
        print(f"    Columns: {result.columns}")
    except Exception as e2:
        print(f"    ✗ Also failed: {e2}")

print("\n" + "=" * 80)
