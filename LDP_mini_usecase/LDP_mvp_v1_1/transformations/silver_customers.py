#silver_customers.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Create the target silver table
dp.create_streaming_table(
  name = "silver_customers",
  expect_all_or_drop = {"no_rescued_data": "_rescued_data IS NULL",
                        "valid_id": "id IS NOT NULL",
                        "valid_category": "customer_category IN ('RED', 'GREEN', 'BLUE')"}
  )

# Create an Append Flow to ingest the raw data into the silver table
@dp.append_flow(
  target = "silver_customers",
  name = "customers_clean_flow"
)

def customers_clean_flow():
  return (
      spark.readStream.table("bronze_customers")
          .select("id",
                  "firstname", 
                  "lastname", 
                  "country_of_origin", 
                  "customer_category", 
                  "email", 
                  F.to_timestamp_ntz(F.col("date_of_birth"),F.lit("MM-dd-yyyy")).cast("date").alias("date_of_birth"),
                  F.to_timestamp_ntz(F.col("registration_date"),F.lit("MM-dd-yyyy HH:mm:ss")).cast("date").alias("registration_date"),
                  F.col("load_ts").cast("timestamp").alias("load_ts"),
                  "_rescued_data")
  )