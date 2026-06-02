from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ==============================================================================
# AUTOMATIC CHANGE DATA CAPTURE (CDC) WITH SCD TYPE 1 & TYPE 2
# ==============================================================================
# In traditional PySpark, implementing Slowly Changing Dimensions (SCD) requires 
# extensive logic (joining target tables, filtering new vs. modified rows, and 
# performing complex merge updates).
#
# Spark Declarative Pipelines introduce `create_auto_cdc_flow` to completely 
# automate this CDC process.
#
# Concepts:
# 1. SCD Type 1: Overwrites historical data directly. The target table always 
#    reflects only the most recent state.
# 2. SCD Type 2: Preserves history by appending new rows and managing metadata 
#    columns (e.g., __start_at, __end_at, __is_current).
# ==============================================================================

# 1. Initialize empty streaming target tables
dp.create_streaming_table(
    name="products_scd1",
    comment="Target table for SCD Type 1 (overwrites historical changes)."
)

dp.create_streaming_table(
    name="products_scd2",
    comment="Target table for SCD Type 2 (preserves historical logs)."
)

# 2. Define Bronze-level Streaming View
# This serves as the unified stream source for both CDC pipelines.
@dp.temporary_view(
    name="products_source",
    comment="Pipeline-scoped source streaming view for CDC parsing."
)
def products_source():
    """
    Reads product transaction logs incrementally from source catalog.
    """
    df = spark.readStream.table("sdp_catalog.source.products")
    return df

# 3. CONSTRUCT SCD TYPE-1 FLOW (Overwrites history)
# When updates occur, rows in 'products_scd1' with matching primary keys are 
# overwritten with the newest sequence state.
dp.create_auto_cdc_flow(
    target="products_scd1",
    source="products_source",
    keys=["product_id"],                      # Unique primary identifier
    sequence_by=col("updated_at"),             # Column to resolve update order (timestamps)
    except_column_list=["updated_at"],        # Columns to exclude from tracking
    stored_as_scd_type="1"                    # Specifies SCD Type 1
)

# 4. CONSTRUCT SCD TYPE-2 FLOW (Preserves history)
# When updates occur, new rows are appended to 'products_scd2', and previous rows 
# are updated with expiration timestamps automatically.
dp.create_auto_cdc_flow(
    target="products_scd2",
    source="products_source",
    keys=["product_id"],                      # Unique primary identifier
    sequence_by=col("updated_at"),             # Resolves sequence ordering
    except_column_list=["updated_at"],        # Exclude updated_at from dimension attributes
    stored_as_scd_type="2"                    # Specifies SCD Type 2 (History Log)
)
