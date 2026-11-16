#silver_customers.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
  name = "silver_customers"
)
@dp.expect(
  "registration_date_not_too_old",
  "registration_date >= DATE '2020-01-01'"
)
@dp.expect_all_or_drop({
    "no_rescued_data": "_rescued_data IS NULL",
    "valid_id": "id IS NOT NULL",
    "valid_category": "customer_category IN ('RED', 'GREEN', 'BLUE')"
})

# AppendFlow implicity created
def silver_customers():
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