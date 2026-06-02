from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ==============================================================================
# DATA QUALITY EXPECTATIONS
# ==============================================================================
# Spark Declarative Pipelines provide built-in constraints (Expectations) 
# to monitor and enforce data quality.
#
# Policies available for quality rules:
# 1. @dp.expect(rule) - Logs violations but permits the records to pass.
# 2. @dp.expect_or_drop(rule) - Drops the failing records and logs it.
# 3. @dp.expect_all_or_fail(rules) - Fails the entire pipeline run if any record violates rules.
# ==============================================================================

# Define a dictionary of data quality constraints (rules)
# Key: Rule name (logged in pipeline history)
# Value: SQL boolean expression that must evaluate to True
quality_rules = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_timestamp": "updated_at IS NOT NULL"
}

@dp.table(
    name="products_table",
    comment="Clean products table audited by strict data quality rules."
)
@dp.expect_all_or_fail(quality_rules)
def products_table():
    """
    Reads product records from the source catalog. Before committing any
    data to disk, it executes the 'quality_rules' verification on all rows.
    If a row fails either rule, the transaction is rolled back and the pipeline fails.
    """
    df = spark.read.table("sdp_catalog.source.products")
    return df
