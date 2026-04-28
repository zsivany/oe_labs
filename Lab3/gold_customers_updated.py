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