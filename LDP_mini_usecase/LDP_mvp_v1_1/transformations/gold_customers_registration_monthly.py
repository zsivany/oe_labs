#gold_customers_registration_monthly.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
  name = "gold_customers_registration_monthly",
  comment = "Customer registrations per month"
)
def customers_registration_monthly():
  return (
      spark.read.table("silver_customers")
        .withColumn("registration_ts", F.to_timestamp("registration_date", "MM-dd-yyyy HH:mm:ss"))
        .withColumn("registration_month", F.date_trunc("MONTH", F.col("registration_ts")))
        .groupBy("registration_month")
        .agg(F.count("*").alias("registered_customers"))
        .orderBy("registration_month")
  )