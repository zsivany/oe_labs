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