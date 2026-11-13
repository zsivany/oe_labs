#gold_customers_categories.py
from pyspark import pipelines as dp

@dp.materialized_view(
  name = "gold_customers_categories",
  comment = "Customer categories ratio"
)
def customers_categories():
  return (
    spark.read.table("silver_customers").groupBy("customer_category").count()
  )