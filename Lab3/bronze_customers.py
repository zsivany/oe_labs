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