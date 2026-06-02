# 🚀 Spark Declarative Pipelines (SDP) & Databricks Lakeflow

Welcome to your personalized **Spark Declarative Pipelines (SDP)** and **Databricks Lakeflow** master reference guide. 

If you state in an interview, design session, or code review that you **"know Spark Declarative Pipelines"**, this document provides all the official definitions, architectural concepts, deep operational details, and real-world system design questions you need to master.

---

## 📖 Table of Contents
1. [Definitions & Ecosystem](#1-definitions--ecosystem)
2. [Core Architectural Blueprint](#2-core-architectural-blueprint)
3. [Declarative vs. Imperative: The Structural Shift](#3-declarative-vs-imperative-the-structural-shift)
4. [Deep-Dive Dataset Specifications](#4-deep-dive-dataset-specifications)
5. [Advanced Patterns (Data Quality, Unioning, CDC)](#5-advanced-patterns-data-quality-unioning-cdc)
6. [Top 15 Advanced Interview Questions & Expert Answers](#6-top-15-advanced-interview-questions--expert-answers)
7. [Codebase Directory Walkthrough](#7-codebase-directory-walkthrough)

---

## 1. Definitions & Ecosystem

To discuss this technology professionally, you must understand how these products are officially positioned in the Databricks Data Intelligence Platform:

*   **Databricks Lakeflow:** The unified, intelligent data engineering suite that covers the entire data lifecycle. It is split into three core pillars:
    1.  **Lakeflow Connect:** Native, serverless connectors for high-throughput, incremental ingestion from SaaS applications (e.g., Salesforce, Workday) and relational databases (e.g., Postgres, MySQL) directly into Bronze tables.
    2.  **Lakeflow Pipelines (formerly Delta Live Tables / DLT):** The declarative transformation framework (using Python or SQL) that automates the orchestration, testing, and incremental processing of tables in a Medallion Architecture.
    3.  **Lakeflow Jobs:** The enterprise workflow orchestration engine that coordinates and monitors end-to-end tasks, notebooks, and external API pipelines.
*   **Spark Declarative Pipelines (SDP):** The specialized, next-generation PySpark SDK (`pyspark.pipelines` or `dlt`) used to develop Lakeflow Pipelines. It allows developers to define target datasets as decorated Python functions and leaves performance tuning, dependency routing, and incremental transaction management to Spark.

---

## 2. Core Architectural Blueprint

### The Control Plane vs. The Compute Plane
When a Lakeflow Pipeline executes, the system splits responsibilities into two distinct architectural areas:
*   **The Control Plane (Databricks Managed Host):** Responsible for parsing your Python code, validating table structures, generating the execution **DAG (Directed Acyclic Graph)**, monitoring running tasks, auditing data quality violation metrics, and auto-scaling active virtual machine clusters. *No raw enterprise customer data ever resides in or passes through the Control Plane.*
*   **The Compute Plane (Customer Cloud VPC / Serverless):** This is where the actual Apache Spark engines compile your physical query plans, read streaming sources, apply UDFs, perform joins/aggregations, and write Delta tables into your cloud storage (S3/ADLS).

```
 ┌────────────────────────────────────────────────────────┐
 │            DATABRICKS CONTROL PLANE (SaaS)             │
 │   - DAG Compilation      - Data Quality Auditing       │
 │   - Auto-scaling Rules   - Observability & Logs        │
 └───────────────────────────┬────────────────────────────┘
                             │ (Secure Orchestration API)
 ┌───────────────────────────▼────────────────────────────┐
 │         CUSTOMER COMPUTE PLANE (Serverless / VPC)      │
 │   - Read Stream Source   - Spark Engine Execution      │
 │   - CDC / Merge Logic    - Write Delta Tables (UC)     │
 └────────────────────────────────────────────────────────┘
```

---

## 3. Declarative vs. Imperative: The Structural Shift

In traditional **Imperative PySpark**, you write procedural code outlining *how* data moves. You must explicitly configure triggers, write checkpoint directories, handle locks, and manually construct `MERGE` commands for updates.

In **Declarative PySpark (SDP)**, you write code describing *what* the targets are. The framework manages the mechanics of lineage, updates, state, and transaction locks.

### Side-by-Side: Change Data Capture (CDC)
Below is an example of what it takes to implement a Slow Changing Dimension (SCD Type 1) update in traditional vs. declarative code.

#### ❌ Imperative PySpark (Traditional Merge)
```python
# Developer must manually manage the join, deduplication, and execution flow
def merge_cdc_updates(batch_df, batch_id):
    # Step 1: Deduplicate updates in the micro-batch using rank/window
    window_spec = Window.partitionBy("product_id").orderBy(col("updated_at").desc())
    deduped_df = batch_df.withColumn("row_num", row_number().over(window_spec)).filter("row_num = 1").drop("row_num")
    
    # Step 2: Manually merge deduped dataframe into target table
    target_table = DeltaTable.forName(spark, "target.products_scd1")
    target_table.alias("t").merge(
        source = deduped_df.alias("s"),
        condition = "t.product_id = s.product_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Step 3: Configure writeStream with structured streaming parameters
query = spark.readStream.table("source.products_raw") \
    .writeStream \
    .format("delta") \
    .foreachBatch(merge_cdc_updates) \
    .option("checkpointLocation", "/mnt/checkpoints/products_cdc") \
    .start()
```

####  Declarative PySpark (SDP)
```python
from pyspark import pipelines as dp

# Step 1: Declare the target streaming table
dp.create_streaming_table("products_scd1")

# Step 2: Define a simple pipeline-scoped streaming view
@dp.temporary_view
def products_source():
    return spark.readStream.table("source.products_raw")

# Step 3: Bind the source to the target with built-in CDC flow
dp.create_auto_cdc_flow(
    target="products_scd1",
    source="products_source",
    keys=["product_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type="1"
)
```

---

## 4. Deep-Dive Dataset Specifications

To design high-performance pipelines, you must master the differences, updates, and cost-implications of the three SDP dataset classes:

| Feature / Metric | Streaming Table (`@dp.table`) | Materialized View (`@dp.materialized_view`) | Temporary View (`@dp.temporary_view`) |
| :--- | :--- | :--- | :--- |
| **Storage Persistence** | Yes. Persisted as a Delta table managed by Unity Catalog. | Yes. Persisted as a Delta table managed by Unity Catalog. | No. Virtual structure exist strictly within compiler runtime memory. |
| **Ingestion Behavior** | **Incremental.** Processes each row exactly once. Ideal for append-only streams. | **Precomputed / Incremental Refreshes.** Computes stateful batch queries or complex joins. | **On-Demand.** Acts as an inline macro; evaluated when downstream nodes query it. |
| **Compute Cost** | **Highly Cost-Efficient.** Processing scales with changes, avoiding re-scans of historical data. | **Medium to High.** Cost depends on join complexity and whether full or incremental updates are run. | **Zero Storage Cost.** Only consumes compute when evaluated by downstream tasks. |
| **Best Use Case** | Bronze layer file ingestion (Auto Loader), append-only web logs, IoT metrics. | Silver/Gold aggregations, customer dimensions, aggregated metrics. | Stream filtering, string cleanups, column renames, schema casting. |

---

## 5. Advanced Patterns (Data Quality, Unioning, CDC)

### Expectations: The Data Quality Firewall
Expectations validate data quality inline inside the streaming pipeline transaction. If rows fail, they are processed based on the decorator policy:

```
                  ┌───────────────────────┐
                  │   Incoming Stream     │
                  └───────────┬───────────┘
                              │
               ┌──────────────▼──────────────┐
               │    Expectation Auditing     │
               └──────────────┬──────────────┘
                              │
             ┌────────────────┼────────────────┐
             │                │                │
    [ @dp.expect ]   [ @dp.expect_or_drop ]   [ @dp.expect_all_or_fail ]
      Log & Pass         Drop Bad Rows         Violated? Rollback
      All Rows           Keep Good Rows        & Fail Pipeline Run
```

*   **`@dp.expect("valid_id", "id IS NOT NULL")`**: Pass all rows. Violations are logged to telemetry metrics for dashboards.
*   **`@dp.expect_or_drop("positive_revenue", "revenue > 0")`**: Keep valid rows. Silently drops violating rows from target table.
*   **`@dp.expect_all_or_fail(rules_dict)`**: Hard block. If even a single record violates a rule, the entire pipeline execution fails.

---

### Change Data Capture (CDC) Internal Mechanics
When using `create_auto_cdc_flow()`, the engine automatically tracks, updates, and structures row modifications.

#### SCD Type 1 vs. SCD Type 2
*   **SCD Type 1:** Always performs an upsert. Historical record state is overwritten.
*   **SCD Type 2:** Keeps a complete log of historical changes. When a record update is received:
    1.  The active record in the target table is expired: its `__end_at` is set to the current sequence value, and `__is_current` is updated to `False`.
    2.  A new row is appended with the updated columns, `__start_at` set to the sequence value, `__end_at` set to `NULL`, and `__is_current` set to `True`.

---

## 6. Top 15 Advanced Interview Questions & Expert Answers

If you are asked technical or architecture-oriented questions about SDP, use these comprehensive, industry-expert answers to demonstrate your mastery:

### Q1: What is the core difference between Declarative Pipelines and Imperative Spark jobs?
**Answer:** 
The core difference lies in the abstraction of orchestration and state management. 
In traditional **imperative PySpark**, the developer is responsible for the "how"—manually coding streaming read triggers, checkpoint directories, write locations, target table merges, data validation checks, and DAG execution sequences. 
In **declarative SDP**, the developer defines "what" the target datasets are (using decorators like `@dp.table` or `@dp.materialized_view`). The underlying execution engine dynamically creates a logical plan, resolves file dependencies to create a physical Directed Acyclic Graph (DAG), tracks checkpoints internally, manages transaction locks, and auto-scales serverless compute resources, reducing operational overhead.

---

### Q2: Under what scenarios should you choose a Streaming Table over a Materialized View?
**Answer:** 
Choose a **Streaming Table** when:
1.  **Ingesting append-only workloads** (such as system logs, clickstream events, or raw storage files via Auto Loader) where processing each row exactly once is critical.
2.  **Minimizing compute cost** is a priority, as it only processes new records incrementally since the last run.
3.  **Low latency** is required.

Choose a **Materialized View** when:
1.  **Querying complex transformations, joins, or aggregations** (e.g., calculating a daily average or combining multiple tables).
2.  **Data updates involve modifications or deletions of historical rows** (since materialized views ensure accuracy by incrementally refreshing or fully recomputing the result state if source datasets change).

---

### Q3: Explain how Spark handles dependency resolution and lineage tracking automatically in SDP.
**Answer:** 
Spark resolves dependencies by parsing the table references inside the decorated functions at compile-time. When a function references a table name via `spark.read.table("table_name")` or `spark.readStream.table("table_name")`, the declarative compiler searches for a decorated dataset matching that specific name within the workspace. 
It maps this relationship to build an in-memory dependency tree, ensuring that upstream datasets are materialized and updated before downstream datasets start processing. This eliminates manual scheduling (e.g., using sleep functions or writing complex multi-task orchestration workflows).

---

### Q4: What are DLT/SDP Expectations? Explain the three policy rules and what happens under-the-hood during a violation.
**Answer:** 
Expectations are declarative assertions for data quality auditing. They are defined as dictionary mappings of SQL-compliant boolean conditions applied as decorators. The three built-in policy rules are:
1.  **`@dp.expect` (Log only):** If a row violates the rule, it is written to the target table normally. The violation event is logged to the pipeline's event log database for reporting.
2.  **`@dp.expect_or_drop` (Filter/Drop):** Violating records are silently filtered out at runtime. The valid records are committed to the target table, and dropped metrics are tracked.
3.  **`@dp.expect_all_or_fail` (Hard Fail):** If a single row violates any rules, the engine aborts the active transaction, rolls back the uncommitted micro-batch, halts pipeline execution, and alerts the monitoring interface.

---

### Q5: How does SDP handle schema evolution in streaming pipelines?
**Answer:** 
SDP manages schema evolution seamlessly when combined with Databricks Auto Loader or Delta Lake. Developers can set schema evolution policies in the pipeline job configuration. When schema changes occur (e.g., new columns are added to raw JSON files):
1.  **Schema Inference/Evolution Mode:** Auto Loader detects the new columns and updates the schema definition in the streaming buffer.
2.  **Delta Schema Merge:** The target streaming table runs a `mergeSchema` operation under-the-hood, appending the new columns as nullable fields in the target Delta table without throwing write-abort errors.

---

### Q6: Explain what `append_flow` is and how it solves the lock-concurrency issue of multiple streams writing to a single table.
**Answer:** 
In traditional Spark Structured Streaming, if you start two concurrent write streams pointing to the exact same Delta table directory, they will trigger transaction conflicts and throw write-lock exceptions. 
SDP solves this via the **Append Flow** pattern:
1.  You declare an empty target Streaming Table once using `dp.create_streaming_table("target_name")`.
2.  You map multiple independent streaming sources to that target using the `@dp.append_flow(target="target_name")` decorator on separate functions.
3.  The engine manages a single execution controller that orchestrates the concurrent micro-batches, preventing write collisions and allowing seamless streaming union operations.

---

### Q7: How does `create_auto_cdc_flow` handle updates under-the-hood? What is the difference in metadata for SCD Type 1 vs SCD Type 2?
**Answer:** 
`create_auto_cdc_flow` compiles an incremental merge query that applies incoming change logs (inserts, updates, deletes) from a streaming source view into a target table.
*   **SCD Type 1:** Compiles a standard Delta `MERGE` query. If a matching primary key is found, the row is overwritten with the newest state. If not found, it is inserted. No historical rows are kept.
*   **SCD Type 2:** Maintains a history of changes by auto-managing metadata tracking columns:
    *   `__start_at`: The timestamp or sequence ID when this record state became active.
    *   `__end_at`: The timestamp or sequence ID when this record state was replaced (null if currently active).
    *   `__is_current`: A boolean flag showing whether this is the active record state (`True` or `False`).
    When an update occurs, the active record is expired (its `__end_at` is set to the current sequence value and `__is_current` is set to `False`), and a new active row is inserted.

---

### Q8: What does the `sequence_by` column do in `create_auto_cdc_flow`? How does it resolve out-of-order records?
**Answer:** 
The `sequence_by` argument (which accepts a Spark DataFrame column reference, typically a transaction timestamp or version ID) is crucial for resolving race conditions and out-of-order data. 
If a micro-batch contains multiple updates for the same primary key, or if an older transaction arrives late due to network latency, the CDC engine uses the `sequence_by` column to order the records. It ensures that only updates with a *higher* sequence value than the existing row in the target table are applied, preventing newer data from being overwritten by older, late-arriving transactions.

---

### Q9: Explain the difference between Control Plane and Compute Plane in the context of Databricks Lakeflow.
**Answer:** 
*   **Control Plane:** The Databricks-managed cloud account layer. It processes the pipeline's Python code, builds the Directed Acyclic Graph (DAG), validates permissions, orchestrates triggers, collects logging/expectations metrics, and displays monitoring graphs in the UI. No customer data resides here.
*   **Compute Plane:** The secure environment (serverless clusters or customer-owned cloud VPC) where the active Spark compute instances reside. This plane compiles physical Spark execution plans, connects to raw source storage, processes transformations, and writes the resulting Delta tables to the database. All data processing occurs here.

---

### Q10: How do you pass runtime arguments/parameters to a Declarative Pipeline?
**Answer:** 
Runtime parameters are passed to a declarative pipeline using the **Pipeline Settings** JSON configuration or job task parameters. In the pipeline code, these values are fetched using the Spark SQL configuration API via `spark.conf.get("parameter_name")`. 
Because the DAG is compiled before execution, parameter values are retrieved during the compilation phase, allowing pipelines to dynamically loop through tables or alter schemas based on configurations.

---

### Q11: What is the role of Unity Catalog in Lakeflow Pipelines?
**Answer:** 
Unity Catalog acts as the centralized governance, security, and metadata layer. In a Lakeflow Pipeline:
1.  All target tables and materialized views are registered directly in the Unity Catalog.
2.  Lineage is captured automatically from source tables to target aggregates, allowing administrators to audit where columns originate.
3.  Granular access controls (select, update, modify permissions) are applied to the generated tables using SQL standard privileges.

---

### Q12: Why doesn't a `@dp.temporary_view` write data to disk? When would you use it instead of a Python function returning a DataFrame?
**Answer:** 
A `@dp.temporary_view` registers a virtual view within Spark's logical query catalog. When compiled, the view's query plan is injected directly into the plan of any downstream table that reads from it. Because it is never materialized to disk, it avoids storage IO bottlenecks.
You use `@dp.temporary_view` instead of a plain Python helper function returning a DataFrame because registering it as a view makes it visible in the compiled **DAG lineage graph**. This makes debugging, lineage tracing, and auditing much easier for operations teams.

---

### Q13: What are the performance and cost benefits of Serverless DLT/Lakeflow Pipelines?
**Answer:** 
Serverless Lakeflow Pipelines provide significant benefits:
1.  **Immediate Compute Availability:** Eliminates the typical 3-5 minute warm-up delay of traditional VM compute clusters.
2.  **Intelligent Auto-Scaling (Enhanced Autoscaling):** Scales compute nodes up or down at a fine-grained, micro-batch level, rather than waiting for long timeout periods, reducing idle compute costs.
3.  **No Infrastructure Overhead:** Databricks handles configuration, patching, and sizing, allowing data engineers to focus solely on writing transformation logic.

---

### Q14: How does incremental compute work for Materialized Views? What triggers a full refresh vs. an incremental update?
**Answer:** 
Incremental compute processes only changes since the last run. 
*   **Incremental Update:** Triggered when the materialized view consists of simple map transformations (select, filter, rename) or simple aggregations on top of streaming table sources. The engine applies change data tracking to merge changes.
*   **Full Refresh:** Triggered manually by a user (via "Full Refresh All" in the UI) or automatically if the query contains transformations that cannot be resolved incrementally (such as complex non-associative window functions, nested non-key joins, or if the schema of the underlying source table has changed significantly).

---

### Q15: If a pipeline fails midway due to a data quality violation (`expect_all_or_fail`), how does SDP ensure ACID transaction consistency?
**Answer:** 
SDP writes all table updates using **Delta Lake transactions**. When a micro-batch processes, the data is written to temporary staging files. 
If an expectation fails (e.g., a row violates a strict `@dp.expect_all_or_fail` rule), the engine stops the active batch run, raises an exception, and discards the staging files. Because no transaction commit is written to the Delta Lake transaction log (`_delta_log/`), the target table remains in its previous state, preventing dirty or corrupted data from being exposed to downstream users.

---

## 7. Codebase Directory Walkthrough

Your workspace contains production-ready, heavily commented code showcasing each of these concepts. Check them out directly to study the practical implementations:

*   📂 **[`spark-declarative-pipelines-guide/`](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/)**
    *   📂 **[`first_sdp_pipeline/`](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/)**
        *   📂 **[`transformations/`](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/)**
            *   📜 **[first_dag.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/first_dag.py)**: Batch Medallion DAGs (`@dp.materialized_view`) for bronze-to-gold workflows.
            *   📜 **[second_dag.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/second_dag.py)**: Streaming/incremental DAGs (`@dp.temporary_view` & `@dp.table`).
            *   📜 **[expectations.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/expectations.py)**: Quality auditing with `@dp.expect_all_or_fail`.
            *   📜 **[append_flow.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/append_flow.py)**: Merges multiple streams into a unified target with `create_streaming_table` and `@dp.append_flow`.
            *   📜 **[auto_cdc.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/auto_cdc.py)**: Tracks updates using SCD Type 1 & 2 via `create_auto_cdc_flow`.
            *   📜 **[parameters.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/parameters.py)**: Orchestrates parameterized loops to declare multiple streams dynamically.
        *   📂 **[`utilities/`](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/utilities/)**
            *   📜 **[utils.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/utilities/utils.py)**: Helper containing a Regex email validation UDF.
        *   📂 **[`explorations/`](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/explorations/)**
            *   📜 **[sample_exploration.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/explorations/sample_exploration.py)**: Interactive notebook used to query and explore the Gold layer sales table.
