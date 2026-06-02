from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ==============================================================================
# MULTI-SOURCE APPEND FLOW (UNION STREAMS)
# ==============================================================================
# In traditional Spark, merging multiple streaming sources requires complex 
# orchestration or multiple write streams which can lock/cause concurrency issues.
#
# In Spark Declarative Pipelines, you can create a single empty streaming table 
# and use @dp.append_flow to seamlessly merge and append multiple inputs.
# ==============================================================================

# 1. Declares an empty target Streaming Table in storage
dp.create_streaming_table(
    name="total_sales",
    comment="Unified streaming table combining data from regional channels (North and South)."
)

# 2. Append flow from North Sales
@dp.append_flow(
    target="total_sales",
    name="north_sales_flow",
    comment="Ingestion stream for Northern division sales."
)
def north_sales():
    """
    Reads records incrementally from the North sales table and
    appends them to the unified 'total_sales' table.
    """
    df = spark.readStream.table("sdp_catalog.source.sales_north")
    return df

# 3. Append flow from South Sales
@dp.append_flow(
    target="total_sales",
    name="south_sales_flow",
    comment="Ingestion stream for Southern division sales."
)
def south_sales():
    """
    Reads records incrementally from the South sales table and
    appends them to the unified 'total_sales' table.
    """
    df = spark.readStream.table("sdp_catalog.source.sales_south")
    return df
