# Databricks notebook source
# MAGIC %md
# MAGIC #<img src="https://databricks.gallerycdn.vsassets.io/extensions/databricks/databricks/2.10.3/1756387992215/Microsoft.VisualStudio.Services.Icons.Default"/></a> Workshop Lab 2 - Databricks#
# MAGIC **Content**
# MAGIC   * Spark DataFrame operations
# MAGIC   * Native Spark functions vs UDF performance benchmark
# MAGIC   * Delta Parquet (features) - Base of the Lakehouse architecture
# MAGIC   * Partitions
# MAGIC   * Tuning / Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ###Advanced Dataframe operations

# COMMAND ----------

# MAGIC %md
# MAGIC Upload the lab dataset - Available on the Moodle
# MAGIC Read into a dataframe

# COMMAND ----------

# Prepartion cell
import pyspark.sql.functions as F
import datetime as datetime
from pyspark.sql.window import Window


# COMMAND ----------

# Read the dataset into the parquet_df variable
# Use Moodle or Git: https://github.com/zsivany/oe_labs/blob/main/Lab2/Lab_2-userdata1.parquet 
# Alternative link:  https://github.com/zsivany/oe_labs/raw/refs/heads/main/Lab2/Lab_2-userdata1.parquet
# Use your own path
parquet_df = spark.read.format("parquet").load('/Volumes/workspace/default/user/Lab_2-userdata1.parquet')

# COMMAND ----------

# Challenge
# Check the schema of the datafrme

# count the dataframe to get some pict about the data


# Display some selected columns and filter on the dataframe and limit it to 20 rows



# COMMAND ----------

# MAGIC %md
# MAGIC ###Some advanced and common dataframe operations

# COMMAND ----------

# Challenge
# Show how many records by country, descreasing order
# Question: What is Finland's ranking in this order? (Not the count value itself)

parquet_df.TODO

#Beware of the differences of sort and orderby and sortWithinPartitions

# COMMAND ----------

# aggregate functions (min, max, avg, sum)
# 
# more options: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html

display(parquet_df.agg(
    F.min(parquet_df.id),
    F.max(parquet_df.id),
    F.avg(parquet_df.id),
    F.sum(parquet_df.id)
))

# Describe command
#parquet_df.describe().display()

# COMMAND ----------

# Schema definition functions
# withColumn, cast, lit functions
# Add new columns and check the data types of the new cols
parquet_df.withColumn('simple_string', F.lit("Constant_with_string")).withColumn('date', F.lit(datetime.datetime.now())).withColumn('str_date', F.col('date').cast('string')).printSchema()

# COMMAND ----------

# Challenge
# Data quality topics
# Handling missing values
# .na function is used for missing values
# .fill(value, col) --> filling missing values with default value
# dropna(subset=cols) --> dropping missing values


# Check for null values on salary columns
#parquet_df.select("salary").where('salary is null').display()

# Check gender values
#parquet_df.select("gender").distinct().display()

# na/dropna in action
#parquet_df.na.replace("", None).dropna(subset="gender").display()

# Try fill to use some default values
# hint use: df.na.fill()
#TODO

#parquet_df.select("salary").na.fill(0).where("salary = 0").count()



# COMMAND ----------

# Window function: perform operations across a set of rows related each other
windowSpec = Window.partitionBy("country").orderBy(F.col("salary").desc())

parquet_df.select("country", "last_name", "salary").withColumn("rank", F.rank().over(windowSpec)).display()


# COMMAND ----------

# MAGIC %md
# MAGIC **HW**
# MAGIC - Add an a sequential / row id to the dataframe
# MAGIC - Expectation: should be unique

# COMMAND ----------

# MAGIC %md
# MAGIC ###Useful methods: dropDuplicates

# COMMAND ----------

# Challenge
# Dropduplicates: Remove the duplicates from the dataframe
# Notice: it should be assign to the "new" dataframe the result if would like to use later
# TODO count parquet_df 

# TODO count parquet_df only unique rows
# TODO count distinct country element

# 3 different numbers what we get
print(number_of_rows)
print(number_distinct_rows)
print(number_distinct_country_rows)


# COMMAND ----------

# MAGIC %md
# MAGIC **HW**
# MAGIC - Check the distinct function

# COMMAND ----------

# MAGIC %md
# MAGIC ###Useful methods: Dataframe to List

# COMMAND ----------

# Challenge
#collect --> put dataframe into a list (with Row object)! 
#Warning it could kill your cluster (CPU heavy operation)
#Very handy operation
#select the user from a country and transform to a list: 

country_list = parquet_df.where(# TODO).collect()

#type(country_list)
#country_list

# Row object element, why is better than simple list? Make a loop for seeing the difference. Hint --> columns:
# 
#for e in country_list:
# TODO




# COMMAND ----------

# MAGIC %md
# MAGIC ###Joins

# COMMAND ----------

#Joins
#Initial example tables
# Employees
emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)

# Departments
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)

deptDF.printSchema()
deptDF.show(truncate=False)

empDF.printSchema()
empDF.show(truncate=False)

# COMMAND ----------

# Challenge
# The joins are very similar as in the SQL and exists all well known mode: innner, left outer, full outer, right outer, anti, etc.
joined_df = empDF.join(deptDF, empDF.dept_id == deptDF.dept_id, "inner")
display(joined_df)



# COMMAND ----------

# MAGIC %md
# MAGIC **HW**
# MAGIC - Try with another join types!

# COMMAND ----------

# MAGIC %md
# MAGIC ###*Explain command*###

# COMMAND ----------

# Explain plan formats
joined_df.explain()

# COMMAND ----------

joined_df.explain(extended = True)

# COMMAND ----------


joined_df.explain(mode="formatted")

# COMMAND ----------

# Great summary about the plans:
#https://medium.com/datalex/sparks-logical-and-physical-plans-when-why-how-and-beyond-8cd1947b605a#:~:text=An%20execution%20plan%20is%20the,optimized%20logical%20and%20physical%20operations.&text=A%20DAG%20is%20an%20acyclic%20graph%20produced%20by%20the%20DAGScheduler%20in%20Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC **HW**
# MAGIC
# MAGIC - Try to write/save out the joined_df with inner join. What's happened? How to solve it?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance comparison - Native Spark Function vs UDF 

# COMMAND ----------

# Prepare some data for the native spark function and udf function comparison
# Exampl get data programmaticaly and use it
import requests
import os
import shutil

# Step 1: download the list of URLs
list_url = "https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/setup_files/raw_data_urls.txt"
response = requests.get(list_url)
urls = response.text.splitlines()

# Step 2: target directory (Unity Catalog Volume)
target_dir = "/Volumes/workspace/default/raw_data/taxi-pq/"
# Clear the target folder before creating it
if os.path.exists(target_dir):
    shutil.rmtree(target_dir)
os.makedirs(target_dir, exist_ok=True)

# Step 3: download only the first 3 files
for url in urls[:3]:
    if not url.strip():
        continue

    filename = url.split("/")[-1]
    filepath = os.path.join(target_dir, filename)
    
    print(f"Downloading {url} → {filepath}")
    
    r = requests.get(url, stream=True)
    with open(filepath, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

print("Download complete!")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/default/raw_data/taxi-pq/

# COMMAND ----------

#Basic check for the dataset
df_taxi = spark.read.format("parquet").load("/Volumes/workspace/default/raw_data/taxi-pq/")
df_taxi.count()
#df_taxi.display()

# COMMAND ----------

# Spark native function --> Check Performance UI and running time
import pyspark.sql.functions as F

spark_df = spark.read.format("parquet").load("/Volumes/workspace/default/raw_data/taxi-pq/")

# Filtering rows
spark_df = spark_df.where("year(Trip_Pickup_DateTime) = 2009")

# Native spark formula (functions) for taxi travel duration
duration_expr = F.expr("(unix_timestamp(Trip_Dropoff_DateTime) - unix_timestamp(Trip_Pickup_DateTime)) / 60")

spark_df = spark_df.withColumn("duration_in_mins", duration_expr)

# Lets roll
spark_df.select("duration_in_mins").describe().display()

# COMMAND ----------

# Spark UDF function --> Check Performance UI and running time
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from datetime import datetime, timedelta

# Define UDF function for taxi travel duration
@F.udf(returnType=DoubleType())
def get_trip_duration(dropoff_datetime: datetime, pickup_datetime: datetime):
    delta = dropoff_datetime - pickup_datetime
    return delta.total_seconds() / 60.0


df = spark.read.format("parquet").load("/Volumes/workspace/default/raw_data/taxi-pq/")

# Filtering rows
df = df.where("year(Trip_Pickup_DateTime) = 2009")

# Adopt the UDF function
df = df.withColumn(
    "duration_in_mins",
    get_trip_duration(
        F.to_timestamp("Trip_Dropoff_DateTime"),
        F.to_timestamp("Trip_Pickup_DateTime")
    )
)

# Lets roll
df.select("duration_in_mins").describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Lake (Delta Parquet)
# MAGIC
# MAGIC Delta Lake is a Spark table with built-in reliability and performance optimizations.
# MAGIC
# MAGIC You can read and write data stored in Delta Lake using the same familiar Apache Spark SQL batch and streaming APIs you use to work with Hive tables or DBFS directories. Delta Lake provides the following functionality:<br><br>
# MAGIC
# MAGIC * <b>ACID transactions</b> - Multiple writers can simultaneously modify a data set and see consistent views.
# MAGIC * <b>DELETES/UPDATES/UPSERTS</b> - Writers can modify a data set without interfering with jobs reading the data set.
# MAGIC * <b>Automatic file management</b> - Data access speeds up by organizing data into large files that can be read efficiently.
# MAGIC * <b>Statistics and data skipping</b> - Reads are 10-100x faster when statistics are tracked about the data in each file, allowing Delta to avoid reading irrelevant information.
# MAGIC * <b>Default file type</b> - is delta from the 8.0 Runtime Enviroment!
# MAGIC * Delta LiveTables
# MAGIC * Delta Autoloader
# MAGIC
# MAGIC *docs: https://delta.io/*

# COMMAND ----------

# helper function for folder cleanup
import os

def ensure_empty_folder(path):
    if os.path.exists(path):
        dbutils.fs.rm(path, True)
    dbutils.fs.mkdirs(path)

ensure_empty_folder('/Volumes/workspace/default/raw_data/taxi-delta/')

# COMMAND ----------

# Delta parquet / delta / databricks delta
# Write out in delta format the earlier dataframe.

parquet_df = spark.read.format("parquet").load("/Volumes/workspace/default/raw_data/taxi-pq/")

parquet_df.write.format("delta").mode("overwrite").save('/Volumes/workspace/default/raw_data/taxi-delta/')

# COMMAND ----------


display(dbutils.fs.ls(f'/Volumes/workspace/default/raw_data/taxi-delta/'))

# COMMAND ----------

#Read the delta table - temp view for sql command
delta_df = spark.read.format("delta").load(f'/Volumes/workspace/default/raw_data/taxi-delta/')
# For the sql API create temp view
delta_df.createOrReplaceTempView("delta_parquet")

# COMMAND ----------

# Challenge
# Lets make some ACID transaction (update, delete)
# There are several way to use --> can use anyone
#delta_df.select("vendor_name").distinct().display()
#delta_df.where("vendor_name = 'VTS' and Passenger_Count = 0").display()
delta_df.where("vendor_name = 'DDS' and Passenger_Count = 0").display()



# COMMAND ----------

#1: Delete
# TODO
spark.sql("DELETE FROM delta_parquet WHERE vendor_name = 'VTS' AND Passenger_Count = 0")

# COMMAND ----------

#2: Update
# TODO
spark.sql("UPDATE delta_parquet set Passenger_Count = -1 WHERE vendor_name = 'DDS' AND Passenger_Count = 0")

# COMMAND ----------

# Check the records which altered


spark.sql("SELECT * FROM delta_parquet WHERE Passenger_Count = -1").show()



# COMMAND ----------

# MAGIC %md
# MAGIC **HW**
# MAGIC - Check and try the upsert operation
# MAGIC - Check the python api on acid operations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Timetravel function in Delta

# COMMAND ----------

# Check the Timetravel 
spark.sql(f"DESCRIBE HISTORY delta.`/Volumes/workspace/default/raw_data/taxi-delta/`").display()

# COMMAND ----------

# Set back to the earlier version
spark.sql(f"RESTORE TABLE delta.`/Volumes/workspace/default/raw_data/taxi-delta/` TO VERSION AS OF 0").display()

# COMMAND ----------

# Challenge
# Check the earlier modified records
spark.sql("SELECT * FROM delta_parquet WHERE Passenger_Count = -1").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##*Partitions*

# COMMAND ----------

# Prepartion cell
# Unity path
partition_parquet = 'workspace.default.partition_parquet'
delta_parquet = 'workspace.default.delta_parquet'
delta_parquet_small = 'workspace.default.delta_parquet_small'

print(partition_parquet)
print(delta_parquet)
print(delta_parquet_small)



# COMMAND ----------

# Prepartion cell
# Clear folders if already exists

spark.sql(f"DROP VOLUME IF EXISTS {delta_parquet}")
spark.sql(f"DROP VOLUME IF EXISTS {delta_parquet_small}")
spark.sql(f"DROP VOLUME IF EXISTS {partition_parquet}")

# COMMAND ----------

# Prepartion cell
# To create the practice folder for reusing purposes anytime
spark.sql(f"CREATE VOLUME IF NOT EXISTS {partition_parquet}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {delta_parquet}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {delta_parquet_small}")

# COMMAND ----------

# Prepartion cell
# Check the folders
spark.sql('SHOW VOLUMES IN workspace.default').show()

# COMMAND ----------

#Repartition - coalesce, 
#PartitionBy - store (performance) -- (Perhaps OneCluster node there is no differences but it is important in the real world!)

# Create testing dataframe
#Lets create a df
#original range: 1000000
from pyspark.sql.functions import *

big_df = spark.range(100_000_000).toDF("number")

# Enrich some extra columns --> low cardinality cols which is good candidate for partitioning
second_df = big_df.withColumn("part", (col("number") % 10).cast("Integer")).withColumn("timestamp", current_timestamp())

second_df.limit(1).display()

# COMMAND ----------

#Case 1: No partition - default configuration
#Write out the data second_df with simple method
# Use the Unity Catalog volume path as required: /Volumes/<catalog>/<schema>/<volume> -- DBFS 
second_df.write.format("parquet").mode("overwrite").save(f'/Volumes/{delta_parquet_small.replace(".", "/")}')

# COMMAND ----------

# We got 8 parquet files - 8 cores
# Each file around less than 1 MB --> not sooo bad but many times could be suboptimal
display(dbutils.fs.ls(f'/Volumes/{delta_parquet_small.replace(".", "/")}'))

# COMMAND ----------

# Discussion
# What if with very small files or too huge?
# How to change the number of partitions?
# We are able to config the partition size:
# repartition or coalesce? What is the key difference?

# COMMAND ----------

#Case 2: Change the partition number
#Notice that the write speed affected!
second_df.repartition(1).write.format("parquet").mode("overwrite").save(f'/Volumes/{delta_parquet_small.replace(".", "/")}')

# COMMAND ----------

display(dbutils.fs.ls(f'/Volumes/{delta_parquet_small.replace(".", "/")}'))

# COMMAND ----------

# MAGIC %md
# MAGIC **HW**
# MAGIC - Try with coalesce() function to reduce partition number

# COMMAND ----------

#Case 3: Set PartitionBy

second_df.write.mode("overwrite").format("parquet").partitionBy("part").save(f'/Volumes/{partition_parquet.replace(".", "/")}')


# COMMAND ----------

# Check file level
display(dbutils.fs.ls(f'/Volumes/{partition_parquet.replace(".", "/")}'))
#display(dbutils.fs.ls(partition_parquet+"part=0"))

# COMMAND ----------

#⚡⚡⚡⚡⚡Lets check some query performance!⚡⚡⚡⚡⚡
parted_df = spark.read.format("parquet").load(f'/Volumes/{partition_parquet.replace(".", "/")}')

# COMMAND ----------

# Challenge
# Normal query - without partition column --> Compare running time
second_df.where("number = 98765").display()


# COMMAND ----------

# Challenge
# Normal query - with partition column
# TODO
parted_df.where("number = 98765 and part=5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tuning and Optimzation
# MAGIC

# COMMAND ----------

# Technique 1
# Storage level
# Small files Optimizer/Vacuum --> Delta Lake

# OPTIMIZE the delta table
# spark.sql(f"OPTIMIZE delta.`{delta_parquet}`").display()
# spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')

# Enable Z order (Delta Lake)
# How to store/sort inside the partition the data --> using frequently used column(s)

# spark.sql("OPTIMIZE table_name ZORDER BY (column_name)")

# Lets check python api the sql command
# Eliminate the not used parquet file and fragments
# spark.sql(f"""VACUUM delta.`{delta_parquet}` RETAIN 00 HOURS""")

# or even simplier if config for the table the autooptimze :
# https://medium.com/@tamaghna.banerjee/delta-lake-optimization-using-auto-optimize-c300baf297d6

# COMMAND ----------

# Technique 2
# Code level
# - Cache dataframes which using more than once (and complicated or big or take long computation time)
# from pyspark.sql import functions as F
# cache_df1 = spark.range(1*10_000_000).toDF("id").withColumn("square", F.col("id")**2)
# cache_df1.printSchema()
# cache_df1.cache() >> [NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000
# cache_df1.count()
# or can check the delta cache option: https://medium.com/@omkarspatil2611/databricks-optimization-technique-delta-cache-52ac6fe22db4


# cache_df1.count()

# COMMAND ----------

# Technique 3
# - Partitioning <--> Bucketing
# Bucketing can reduce or avoid shuffle some cases like join

# print(spark.conf.get("spark.sql.sources.bucketing.enabled"))

# example: 
# df.write\
# .bucketBy(32, 'joining_key') \  # number of buckets --> depends on size, bucketing key
# .sortBy('date_created') \ # somer order need
# .saveAsTable('bucketed', format='parquet')

# COMMAND ----------

# Technique 4
# Several Join types
# Broadcast Hash Join
# Reduce Shuffle, boost up performance
# Small dataset can be distributed all worker.

#from pyspark.sql.functions import broadcast

# Perform the join with broadcast
# result_df = big_df.join(broadcast(small_df), "join_column")

# Additional can set the broadcast spark config. Hint: Up to 100MB if we have quite strong workers
# 20 Mb threshold --> spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20 * 1024 * 1024)

# COMMAND ----------

# AQE is especially useful in these scenarios:

# Complex queries with multiple joins, aggregations, or window functions.
# Situations where you experience data skew.
# Large datasets with imbalanced partition sizes.

# All in ALL you can switch anytime

# spark.conf.set("spark.sql.adaptive.enabled", "true")

# https://spark.apache.org/docs/latest/sql-performance-tuning.html

# https://medium.com/@manoj.kdas37/how-to-optimize-your-apache-spark-jobs-top-10-approaches-and-best-practices-for-performance-tuning-4630ae864f52



# COMMAND ----------

spark.sql("SHOW SCHEMAS").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Extra dataframe ops..

# COMMAND ----------

# Fun with datetimes!
# lit: able to add consant col to the dataframe, can be any datatype
# cast: can set the data types of the cols

import datetime

display(parquet_df.withColumn('simple_date_string', F.lit("Constant_with_string")).withColumn('date', F.lit(datetime.datetime.now())).withColumn('str_date', F.col('date').cast('string')))

# COMMAND ----------

# String ops:
# Substring as usual: string operation to cut of a definitive slice from the string
# Try substring function or concat_ws
# TODO
parquet_df.select("first_name", "last_name").withColumn("full_name", F.concat_ws(" ", "first_name", "last_name")).drop("first_name", "last_name").display()

# COMMAND ----------

# Handy dataframe ops
#withcolumn: Add col to the dataframe
#regexp_replace: replace character in the col
#parquet_df.select("ip_address").display()
parquet_df.select("ip_address").withColumn("mod_ip_address", F.regexp_replace("ip_address", "\.", "-")).display()


