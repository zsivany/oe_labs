#gold_customers_updated.py
from pyspark import pipelines as dp

@dp.materialized_view(
  name = "customers_updated",
  comment = "Only the updated customers"
)
def customers_updated():
  return (
    spark.read.table("silver_customers").where("operation = 'UPDATE'")

  )