#gold_customers_by_country.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
  name = "gold_customers_by_country",
  comment = "Total customers and dominant customer category per country of origin"
)
def customers_by_country():
  counts = (
      spark.read.table("silver_customers")
        .groupBy("country_of_origin", "customer_category")
        .agg(F.count("*").alias("category_count"))
  )
  w = Window.partitionBy("country_of_origin").orderBy(F.desc("category_count"))
  return (
      counts
        .withColumn("total_customers", F.sum("category_count").over(Window.partitionBy("country_of_origin")))
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select(
            "country_of_origin",
            "total_customers",
            F.col("customer_category").alias("dominant_category"),
            "category_count"
        )
  )