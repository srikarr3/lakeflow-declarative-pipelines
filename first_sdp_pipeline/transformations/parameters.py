from pyspark import pipelines as dp 
import ast 

# ==============================================================================
# PARAMETERIZED & DYNAMIC PIPELINE DEFINITION
# ==============================================================================
# Declarative pipelines support runtime parameterization. You can fetch variables 
# passed from Databricks job configurations using `spark.conf.get()`.
#
# This allows dynamic creation of streaming targets in loops, giving you immense
# flexibility when processing multiple similar landing tables.
# ==============================================================================

# Fetch the string representation of a Python list from Spark Configuration
# Expected input format: '["region_a", "region_b", "region_c"]'
list_var = spark.conf.get("tables_list", defaultValue="['default']")

# Safely evaluate the string representation of a list into a Python list object
list_var_list = ast.literal_eval(list_var)

# Dynamically generate a streaming table for each value in the list
for table_name_suffix in list_var_list:
    
    # We define a table factory closure to avoid variable scoping issues
    def create_dynamic_table(suffix):
        @dp.table(
            name=f"table_{suffix}",
            comment=f"Dynamically generated streaming table for division: {suffix}."
        )
        def dynamic_ingestion_stream():
            """
            Reads from the base sales table as a stream and isolates it 
            into a dynamically declared workspace partition.
            """
            df = spark.readStream.table("sdp_catalog.source.sales")
            return df
            
    create_dynamic_table(table_name_suffix)
