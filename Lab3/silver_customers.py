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