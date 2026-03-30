# Fabric Mirror Serving - Quick Start Guide

## Installation

### For Local Development/Testing (Before Official Release)

```bash
# 1. Create and activate a Python virtual environment in your project
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

# 2. Install azure-cosmos from the fabric-mirror-mapper-poc branch
pip install git+https://github.com/TheovanKraay/azure-sdk-for-python.git@fabric-mirror-mapper-poc#subdirectory=sdk/cosmos/azure-cosmos

# 3. Install azure-cosmos-fabric-mapper from the 001-fabric-mirror-mapper branch with SQL driver
pip install "git+https://github.com/TheovanKraay/azure-cosmos-fabric-mapper.git@001-fabric-mirror-mapper#egg=azure-cosmos-fabric-mapper[sql]"
```

**Alternative: Install from local clones for development**

```bash
# If you want to make changes and test them
# 1. Create and activate virtual environment (as above)

# 2. Clone and install azure-cosmos in editable mode
git clone -b fabric-mirror-mapper-poc https://github.com/TheovanKraay/azure-sdk-for-python.git
pip install -e azure-sdk-for-python/sdk/cosmos/azure-cosmos

# 3. Clone and install azure-cosmos-fabric-mapper in editable mode
git clone -b 001-fabric-mirror-mapper https://github.com/TheovanKraay/azure-cosmos-fabric-mapper.git
pip install -e azure-cosmos-fabric-mapper[sql]
```

**Note:** Both packages must be from their respective branches:
- `azure-sdk-for-python`: **fabric-mirror-mapper-poc** branch
- `azure-cosmos-fabric-mapper`: **001-fabric-mirror-mapper** branch

### For Production Use (After Official Release)

```bash
# Install Cosmos SDK from PyPI
pip install azure-cosmos

# Install Fabric mapper with SQL driver
pip install azure-cosmos-fabric-mapper[sql]
```

### Quick Verification

After installation, verify mirror serving is available:

```python
from azure.cosmos import CosmosClient

# Check if mirror serving imports work
try:
    from azure.cosmos._mirror_integration import execute_mirrored_query
    print("✓ Mirror serving is available!")
except ImportError:
    print("✗ Mirror serving not available - check installation")
```

## Basic Usage

**Prerequisites:**
- Azure Cosmos DB account with Fabric mirroring enabled
- Fabric warehouse with mirrored data
- Azure credentials (DefaultAzureCredential or similar)

```python
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()

# Create client with mirror configuration
client = CosmosClient(
    url="https://your-account.documents.azure.com:443/",
    credential=credential,
    mirror_config={
        "server": "your-warehouse.datawarehouse.fabric.microsoft.com",
        "database": "your-database",
        "credential": credential
    }
)

database = client.get_database_client("your-database")
container = database.get_container_client("your-container")

# Route queries individually using use_mirror_serving parameter

# Point read from Cosmos DB (fast, low latency)
item = container.query_items(
    query="SELECT * FROM c WHERE c.id = @id",
    parameters=[{"name": "@id", "value": "123"}],
    use_mirror_serving=False  # or omit (default=False)
)

# Aggregation from Fabric mirror (cheaper, supports GROUP BY)
results = container.query_items(
    query="""
        SELECT c.category, COUNT(1) as itemCount, AVG(c.price) as avgPrice
        FROM c
        WHERE c.status = 'active'
        GROUP BY c.category
    """,
    use_mirror_serving=True  # Route to Fabric mirror
)

for item in results:
    print(f"{item['category']}: {item['itemCount']} items, avg ${item['avgPrice']:.2f}")
```

## When to Use Mirror Serving

### ✅ USE FABRIC MIRROR for:

1. **GROUP BY queries** (not supported in Cosmos DB Python SDK!)
   ```sql
   SELECT c.region, COUNT(1), AVG(c.sales)
   FROM c
   GROUP BY c.region
   ```
   **Usage:** `container.query_items(query=..., use_mirror_serving=True)`

2. **Complex aggregations across partitions**
   ```sql
   SELECT 
       c.department,
       COUNT(1) as total,
       SUM(c.revenue) as totalRevenue,
       AVG(c.satisfaction) as avgSatisfaction
   FROM c
   GROUP BY c.department
   ```
   **Usage:** `container.query_items(query=..., use_mirror_serving=True)`

3. **ORDER BY for analytical queries**
   ```sql
   SELECT TOP 100 c.product, c.sales
   FROM c
   ORDER BY c.sales DESC
   ```
   **Usage:** `container.query_items(query=..., use_mirror_serving=True)`

4. **Data warehouse-style reporting**
   - Multi-partition aggregations
   - Complex analytical queries
   - Large result sets with filtering and sorting
   
   **Usage:** `container.query_items(query=..., use_mirror_serving=True)`

### ⚠️ USE COSMOS DB DIRECT for:

1. **Point lookups** (partition key + ID)
   ```python
   container.read_item(item="doc-123", partition_key="2024-01-01")
   # or
   container.query_items(
       query="SELECT * FROM c WHERE c.partitionKey = @pk AND c.id = @id",
       parameters=[...],
       use_mirror_serving=False  # or omit
   )
   ```

2. **Simple queries with small result sets**
   ```sql
   SELECT * FROM c WHERE c.partitionKey = 'user-123' AND c.type = 'order'
   ```
   **Usage:** `container.query_items(query=..., use_mirror_serving=False)` or omit parameter

3. **Low-latency requirements** (<100ms)
   - Real-time applications
   - Interactive UIs
   - Single-partition queries
   
   **Usage:** `container.query_items(query=..., use_mirror_serving=False)` or omit parameter

## Environment Variable Support

```bash
# Enable mirror serving globally
export COSMOS_ENABLE_MIRROR_SERVING=true

# Then just provide mirror_config
client = CosmosClient(
    url="https://your-account.documents.azure.com:443/",
    credential=credential,
    mirror_config={...}
)
```

## Error Handling

```python
from azure.cosmos._mirror_integration import MirrorServingNotAvailableError

try:
    results = container.query_items(query="SELECT c.region, COUNT(1) FROM c GROUP BY c.region")
    for item in results:
        print(item)
        
except MirrorServingNotAvailableError as e:
    print(f"Mirror serving not available: {e}")
    print("To enable: pip install azure-cosmos-fabric-mapper[sql]")
    
except Exception as e:
    print(f"Query failed: {e}")
```

## Configuration Reference

### Mirror Config Parameters

```python
mirror_config = {
    # Required: Fabric warehouse server FQDN
    "server": "your-warehouse.datawarehouse.fabric.microsoft.com",
    
    # Required: Database name (also used as schema in Fabric)
    "database": "your-database-name",
    
    # Required: Azure credential for authentication
    "credential": DefaultAzureCredential(),
    
    # Optional: Override table name (defaults to container name)
    "table_override": "custom-table-name"
}
```

## Common Patterns

### Pattern 1: Analytical Dashboard

```python
# Get summary statistics by region
summary_query = """
    SELECT 
        c.region,
        COUNT(1) as totalOrders,
        SUM(c.amount) as totalRevenue,
        AVG(c.amount) as avgOrderValue
    FROM c
    WHERE c.orderDate >= @startDate
    GROUP BY c.region
    ORDER BY totalRevenue DESC
"""

results = container.query_items(
    query=summary_query,
    parameters=[{"name": "@startDate", "value": "2024-01-01"}]
)

for region_stats in results:
    print(f"{region_stats['region']}: "
          f"{region_stats['totalOrders']} orders, "
          f"${region_stats['totalRevenue']:,.2f} revenue")
```

### Pattern 2: Top N Analysis

```python
# Find top 10 products by sales
top_products_query = """
    SELECT TOP 10
        c.productId,
        c.productName,
        SUM(c.quantity) as totalQuantity,
        SUM(c.revenue) as totalRevenue
    FROM c
    WHERE c.partitionKey = @date
    GROUP BY c.productId, c.productName
    ORDER BY totalRevenue DESC
"""

results = container.query_items(
    query=top_products_query,
    parameters=[{"name": "@date", "value": "2024-01-15"}]
)

print("Top 10 Products:")
for i, product in enumerate(results, 1):
    print(f"{i}. {product['productName']}: ${product['totalRevenue']:,.2f}")
```

### Pattern 3: Time Series Aggregation

```python
# Aggregate metrics by hour
hourly_metrics = """
    SELECT 
        c.hour,
        COUNT(1) as eventCount,
        AVG(c.responseTime) as avgResponseTime,
        MAX(c.responseTime) as maxResponseTime
    FROM c
    WHERE c.partitionKey = @date
    GROUP BY c.hour
    ORDER BY c.hour
"""

results = container.query_items(
    query=hourly_metrics,
    parameters=[{"name": "@date", "value": "2024-01-15"}]
)

for hourly in results:
    print(f"Hour {hourly['hour']:02d}: "
          f"{hourly['eventCount']} events, "
          f"avg response {hourly['avgResponseTime']:.0f}ms")
```

## Troubleshooting

### Issue: "Mirror serving not available"

**Solution:** Install the mapper package with SQL driver:
```bash
pip install azure-cosmos-fabric-mapper[sql]
```

### Issue: "Table not found"

**Possible causes:**
1. Container not mirrored to Fabric
2. Mirror replication not complete
3. Incorrect database/table names

**Check:** Verify mirror is configured in Azure Portal → Cosmos DB → Mirror → Check status

### Issue: Queries still fail with "not supported"

**Possible causes:**
1. `enable_mirror_serving=True` not set
2. `mirror_config` missing or incomplete
3. Environment variable not set

**Check:** Verify client configuration includes both parameters

### Issue: Different results between Cosmos and Mirror

**Expected behavior:** Mirror serving routes ALL queries when enabled, not just GROUP BY.

**Solution:** Test queries on Cosmos first (without mirror) to establish baseline, then enable mirror.

## Performance Tips

1. **Use Fabric mirror for:**
   - Queries scanning multiple partitions
   - GROUP BY and ORDER BY queries
   - Large aggregations (>10K items)

2. **Use Cosmos direct for:**
   - Single partition queries
   - Point reads (partition key + ID)
   - Small result sets (<100 items)
   - Low latency requirements

3. **Optimize queries:**
   - Always include partition key filter when possible
   - Limit result sets with TOP or pagination
   - Use appropriate indexes in Fabric warehouse

## Best Practices

1. **✅ DO:**
   - Use mirror serving for analytical/reporting workloads
   - Test queries thoroughly before production
   - Monitor query performance and costs
   - Use parameterized queries to prevent SQL injection

2. **❌ DON'T:**
   - Use mirror serving for OLTP workloads (point reads/writes)
   - Assume identical performance (mirror has higher latency)
   - Forget to handle MirrorServingNotAvailableError
   - Hard-code credentials (use DefaultAzureCredential)

## Resources

- [Azure Cosmos DB Documentation](https://docs.microsoft.com/azure/cosmos-db/)
- [Azure Fabric Documentation](https://docs.microsoft.com/fabric/)
- [azure-cosmos-fabric-mapper GitHub](https://github.com/Azure/azure-cosmos-fabric-mapper)
- [Cosmos DB Mirror Feature](https://docs.microsoft.com/azure/cosmos-db/mirror)

## Support

For issues or questions:
1. Check this guide first
2. Review the error messages carefully
3. Check Azure Portal for mirror status
4. Open issue on GitHub: azure-sdk-for-python repository
