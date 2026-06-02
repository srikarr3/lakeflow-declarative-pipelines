# 🛠️ Spark Declarative Pipelines - Production Source Code

This directory contains the complete declarative source code and configuration for your **Spark Declarative Pipeline**:

*   📂 **`transformations`**: Contains all declarative dataset, table, view, CDC, and validation definitions.
*   📂 **`utilities`**: Custom Python modules and Spark UDFs used by the transformation pipelines.
*   📂 **`explorations`**: Ad-hoc interactive notebooks for data exploration, prototyping, and analysis.

---

## ⚡ Getting Started

The transformations defined in this pipeline are ready to be deployed as a **Databricks Lakeflow / Delta Live Tables (DLT)** pipeline:

1.  **Transformations Directory:** View the core declarative configurations under the `transformations/` folder.
2.  **Batch Processing:** See [first_dag.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/first_dag.py) for the structured Batch Medallion model.
3.  **Streaming & Incremental ETL:** View [second_dag.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/second_dag.py) for the Streaming/Incremental model.
4.  **Quality Auditing:** See [expectations.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/expectations.py) to check how rules are validated.
5.  **Change Data Capture (CDC):** Review [auto_cdc.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/auto_cdc.py) for automated Slowly Changing Dimensions (SCD Type 1 & 2) tracking.
6.  **Pipeline Union/Merge:** Check out [append_flow.py](file:///c:/Users/manda/OneDrive/Desktop/Notes/spark-declarative-pipelines-guide/first_sdp_pipeline/transformations/append_flow.py) to see how multi-source streams append to a single target table.

---

## 🚀 Execution Instructions

*   To execute the entire pipeline, load this workspace into your Databricks Repo and select **Run pipeline** in the DLT/Declarative Pipeline UI.
*   Individual files can be run or previewed using **Run file**.