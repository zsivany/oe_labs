# Databricks notebook source
# MAGIC %md
# MAGIC #<img src="https://img.stackshare.io/service/10345/ADB.png"/></a> Workshop Lab 3 - Databricks LDP (DLT)#
# MAGIC **Content**
# MAGIC   * Pipeline - Mini usecase
# MAGIC   * ML session
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC # Data processing framework
# MAGIC
# MAGIC * Agenda
# MAGIC * Key termonologies
# MAGIC * GUI check
# MAGIC * MiniUsecase - Medallion with Lakeflow
# MAGIC     * create pipeline (Consists: 4x pipeline codes, 1x notebook cell for data generation)
# MAGIC     * run, check the results
# MAGIC     * rerun
# MAGIC * Playing with Genie and SQL Editor
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧭 Terminology Jungle
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Core Concepts**
# MAGIC
# MAGIC **Orchestration**  
# MAGIC The coordination and scheduling of multiple data processing tasks or workflows to ensure they run in the correct order and timing.
# MAGIC
# MAGIC **Job**  
# MAGIC A single, executable unit of work in Databricks (e.g., running a notebook, script, or pipeline) that can be scheduled or triggered.
# MAGIC
# MAGIC **Pipeline**  
# MAGIC A defined sequence of data transformations and movement steps (e.g., ETL/ELT) that process data from source to target, often managed and executed through orchestration.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Declarative Frameworks**
# MAGIC
# MAGIC **DLT – Delta Live Tables ←→ Lakeflow Declarative Pipelines**
# MAGIC
# MAGIC **Delta Live Tables (DLT)**  
# MAGIC *A declarative framework for building reliable, maintainable, and testable data processing pipelines.*
# MAGIC
# MAGIC **Lakeflow Declarative Pipelines (LDP)**  
# MAGIC *A declarative framework for developing and running batch and streaming data pipelines in SQL and Python.*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Approach Styles**
# MAGIC
# MAGIC **Procedural approach**  
# MAGIC You define *how* tasks should run by specifying the exact sequence of operations and control flow.
# MAGIC
# MAGIC **Declarative approach**  
# MAGIC You describe *what* result you want, and the system figures out how to execute it optimally.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Pipeline Outputs**
# MAGIC
# MAGIC **Sink**  
# MAGIC A target destination (e.g., Delta table, Kafka topic, Event Hub) for streaming flows where processed records are written out via a pipeline.
# MAGIC
# MAGIC **Streaming table**  
# MAGIC A Unity Catalog–managed table defined for incremental or streaming ingestion and processing, backed by a serverless pipeline, and refreshed on a schedule or trigger.  
# MAGIC
# MAGIC **Materialized view**  
# MAGIC A persistent table that stores the precomputed results of a query, updates incrementally or on schedule, and provides faster query performance than a standard view.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <div align="center">
# MAGIC   <img src="https://docs.databricks.com/aws/en/assets/images/dlt-core-concepts-6bc9894d3682035cadac19f3980875ae.png" width="800"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC <div align="center">
# MAGIC <img src="https://miro.medium.com/v2/resize:fit:4800/format:webp/1*MggtENQXxtieCKA30b4xbg.png" width="800"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Mini Usecase
# MAGIC ##Steps:
# MAGIC   * Emulate the data source with Notebook cell
# MAGIC   * Create medallion layers via Lakeflow Pipelines (DLT) (bronze, silver, gold):
# MAGIC   * Run it and test
# MAGIC

# COMMAND ----------

#Step 0 - Generate some source data
%pip install faker
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict
import uuid
import random

# Update these to match the catalog and schema
# that you used for the pipeline in step 1.
catalog = "workspace"
schema = dbName = db = "default"

spark.sql(f'USE CATALOG `{catalog}`')
spark.sql(f'USE SCHEMA `{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{db}`.`raw_data`')
volume_folder =  f"/Volumes/{catalog}/{db}/raw_data"

try:
  dbutils.fs.ls(volume_folder+"/customers")
except:
  print(f"folder doesn't exist, generating the data under {volume_folder}...")

fake = Faker()
# docs: https://github.com/xfxf/faker-python

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
operations = OrderedDict([("APPEND", 0.5),("DELETE", 0.1),("UPDATE", 0.3),(None, 0.01)])
fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)

number_of_customer = 7664
df = spark.range(0, number_of_customer).repartition(20)

df = df.withColumn("id", fake_id())
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("operation", fake_operation())
df_customers = df.withColumn("operation_date", fake_date())
print(df_customers.count())
df_customers.repartition(20).write.format("json").mode("append").save(volume_folder+"/customers")
  
print(f"{number_of_customer} customers are generated. Process done..")

# COMMAND ----------

# Challenge
# Check the data

df = spark.read.format("json").load(volume_folder+"/customers")
display(df)

# COMMAND ----------

#PIPELINE Code snippets

# COMMAND ----------

#bronze_customers.py
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Source config
path = "/Volumes/workspace/default/raw_data/customers/"

# Create the target bronze table
dp.create_streaming_table(name = "bronze_customers", 
                          comment = "New customer data incrementally ingested from cloud object storage landing zone")

# Create an Append Flow to ingest the raw data into the bronze table
@dp.append_flow(
  target = "bronze_customers",
  name = "bronze_customers_ingest_flow"
)
# readStream function can track automatically the loaded files
def bronze_customers_ingest_flow():
  return (
      spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .load(f"{path}")
  )

# COMMAND ----------

#silver_customers.py
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Create the target silver table
dp.create_streaming_table(
  name = "silver_customers",
  expect_all_or_drop = {"no_rescued_data": "_rescued_data IS NULL",
                        "valid_id": "id IS NOT NULL",
                        "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"}
  )

# Create an Append Flow to ingest the raw data into the silver table
@dp.append_flow(
  target = "silver_customers",
  name = "customers_clean_flow"
)

def customers_clean_flow():
  return (
      spark.readStream.table("bronze_customers")
          .select("address", "email", "id", "firstname", "lastname", "operation", "operation_date", "load_ts","_rescued_data")
  )

# COMMAND ----------

#gold_customers_agg.py
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
  name = "customers_agg",
  comment = "Aggregated customer records"
)
def customers_history_agg():
  return (
    spark.read.table("silver_customers")
      .groupBy("id")
      .agg(
          count("address").alias("address_count"),
          count("email").alias("email_count"),
          count("firstname").alias("firstname_count"),
          count("lastname").alias("lastname_count")
      )
  )

# COMMAND ----------

#gold_customers_updated.py
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
  name = "customers_updated",
  comment = "Only the updated customers"
)
def customers_updated():
  return (
    spark.read.table("silver_customers").where("operation = 'UPDATE'")

  )

# COMMAND ----------

# MAGIC %md
# MAGIC #Q&A
# MAGIC
# MAGIC
# MAGIC contacts: tamas.papp@t-systems.com, 
# MAGIC          agnes.gerse@t-systems.com

# COMMAND ----------

# MAGIC %md
# MAGIC Docs:
# MAGIC - https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started
# MAGIC - https://youtu.be/krzr52wd4nM
# MAGIC - https://docs.databricks.com/aws/en/data-engineering/#gsc.tab=
# MAGIC - https://docs.databricks.com/aws/en/assets/images/dlt-core-concepts-6bc9894d3682035cadac19f3980875ae.png
# MAGIC - https://docs.databricks.com/aws/en/ldp/concepts#gsc.tab=0
# MAGIC - https://docs.databricks.com/aws/en/ldp/develop#gsc.tab=0
