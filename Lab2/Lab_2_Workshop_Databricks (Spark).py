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

# Challenge - TODO
# Check the schema of the datafrme
# count the dataframe to get some pict about the data
# Display some selected columns and filter on the dataframe and limit it to 20 rows


# COMMAND ----------

# MAGIC %md
# MAGIC ###Some advanced and common dataframe operations

# COMMAND ----------

# Challenge - TODO
# Show how many records by country, descreasing order
# Question: What is Finland's ranking in this order? (Not the count value itself)


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

# Challenge - TODO
# Data quality topics
# Handling missing values
# .na function is used for missing values
# .fill(value, col) --> filling missing values with default value
# dropna(subset=cols) --> dropping missing values


# Check for null values on salary columns
#parquet_df.select("salary").where('salary is null').count()

# Check gender values
#parquet_df.select("gender").distinct().display()

# na/dropna in action
#parquet_df.na.replace("", None).dropna(subset="gender").select("gender").distinct().display()

# Try fill to use some default values
# hint use: df.na.fill()
#TODO



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

# Spark native function --> Check Performance UI/Spark UI and running time
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

# Spark UDF function --> Check Performance UI/Spark UI and running time
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
# MAGIC * Delta LiveTables - Lakehouse Declarative Pipeline
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


# COMMAND ----------

#2: Update
# TODO


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
#TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ##*Partitions*

# COMMAND ----------

ensure_empty_folder('/Volumes/workspace/default/raw_data/taxi-delta-partition/')
ensure_empty_folder('/Volumes/workspace/default/raw_data/taxi-delta-partitionby/')

# COMMAND ----------

#Repartition - coalesce and others
#PartitionBy - store (performance) -- (Perhaps OneCluster node there is no differences but it is important in the real world!)

partition_df = spark.read.format("delta").load(f'/Volumes/workspace/default/raw_data/taxi-delta/')

#Check the data
partition_df.display()

# COMMAND ----------

# Case 1: No partition - default configuration
# Use the Unity Catalog volume path as required: /Volumes/<catalog>/<schema>/<volume> -- DBFS 
# Investigate the files and sizes
display(dbutils.fs.ls(f'/Volumes/workspace/default/raw_data/taxi-delta/'))

# COMMAND ----------

# Discussion
# What if with very small files or too huge?
# How to change the number of partitions?
# We are able to config the partition size:
# repartition or coalesce? What is the key difference?

# COMMAND ----------

#Case 2: Change the partition number
#Notice that the write speed affected or not
#Question: repartition number depends on the number of cores?
# repartition or coalesce? What is the key difference?
# repartition is a shuffle operation and coalesce is not. Repartition is used to increase or decrease the number of partitions. Coalesce is used to decrease the number of partitions.
# change the number of partitions!
# TODO
partition_df.repartition(8).write.mode("overwrite").save('/Volumes/workspace/default/raw_data/taxi-delta-partition/')

# COMMAND ----------

display(dbutils.fs.ls('/Volumes/workspace/default/raw_data/taxi-delta-partition/'))

# COMMAND ----------

# MAGIC %md
# MAGIC **HW**
# MAGIC - Try with coalesce() function to reduce partition number

# COMMAND ----------

#Case 3: Set PartitionBy
# change the partition col(s)! Watch the cardinality!
# TODO
partition_df.write.mode("overwrite").partitionBy("vendor_name").save('/Volumes/workspace/default/raw_data/taxi-delta-partitionby')



# COMMAND ----------

# Check file level
display(dbutils.fs.ls('/Volumes/workspace/default/raw_data/taxi-delta-partitionby'))

# COMMAND ----------

#⚡⚡⚡⚡⚡Lets check some query performance!⚡⚡⚡⚡⚡
repartition_df = spark.read.load('/Volumes/workspace/default/raw_data/taxi-delta-partition')
partitionby_df = spark.read.format("delta").load('/Volumes/workspace/default/raw_data/taxi-delta-partitionby')

# COMMAND ----------

# Challenge
# Normal query - without partition column --> Compare running time
repartition_df.where("vendor_name = 'VTS' AND Passenger_Count = 0").display()


# COMMAND ----------

# Challenge
# Normal query - with partition column
# Analyze the performance UI / Spark UI
# TODO
partitionby_df.where()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tuning and Optimzation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Technique 1
# MAGIC **Storage level**
# MAGIC
# MAGIC - Small files Optimizer/Vacuum → Delta Lake
# MAGIC
# MAGIC - OPTIMIZE the delta table  
# MAGIC   `spark.sql(f"OPTIMIZE delta.`{delta_parquet}`").display()`
# MAGIC
# MAGIC - `spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')`
# MAGIC
# MAGIC - Enable Z order (Delta Lake)  
# MAGIC   How to store/sort inside the partition the data → using frequently used column(s)
# MAGIC
# MAGIC - `spark.sql("OPTIMIZE table_name ZORDER BY (column_name)")`
# MAGIC
# MAGIC - Lets check python api the sql command  
# MAGIC   Eliminate the not used parquet file and fragments  
# MAGIC   `spark.sql(f"""VACUUM delta.`{delta_parquet}` RETAIN 00 HOURS""")`
# MAGIC
# MAGIC - Or even simpler if config for the table the autooptimze:  
# MAGIC   [Delta Lake Optimization using Auto Optimize](https://medium.com/@tamaghna.banerjee/delta-lake-optimization-using-auto-optimize-c300baf297d6)
# MAGIC
# MAGIC - [Liquid Clustering: the latest strategy offered by Databricks](https://docs.databricks.com/aws/en/delta/clustering) --> but this is table level
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 2: Caching DataFrames for Performance Optimization
# MAGIC
# MAGIC - **Cache DataFrames** that are used multiple times, are large, or require expensive computations.
# MAGIC - Use `cache()` for Spark DataFrames.  
# MAGIC   *Note: `cache()` is not supported on serverless compute.*
# MAGIC - For Delta tables, consider enabling **Delta Cache** for further optimization.
# MAGIC - [Reference: Databricks Optimization Technique - Delta Cache](https://medium.com/@omkarspatil2611/databricks-optimization-technique-delta-cache-52ac6fe22db4)
# MAGIC
# MAGIC **Example:**
# MAGIC
# MAGIC `from pyspark.sql import functions as F`
# MAGIC
# MAGIC `cache_df1 = spark.range(10_000_000).toDF("id").withColumn("square", F.col("id")**2)`
# MAGIC
# MAGIC `cache_df1.cache()`
# MAGIC
# MAGIC `cache_df1.count()`

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 3: Join Optimization
# MAGIC
# MAGIC ### Join Types
# MAGIC - **Broadcast Hash Join:**  
# MAGIC   - Reduces shuffle and boosts performance.
# MAGIC   - Small dataset can be distributed to all worker nodes.
# MAGIC
# MAGIC #### Example: Broadcast Join in PySpark
# MAGIC python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC Perform the join with broadcast
# MAGIC - `result_df = big_df.join(broadcast(small_df), "join_column")`
# MAGIC
# MAGIC
# MAGIC #### Spark Configuration
# MAGIC - You can set the broadcast threshold (e.g., up to 100MB if you have strong workers):
# MAGIC - %python
# MAGIC - `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20 * 1024 * 1024)`  # 20 MB threshold

# COMMAND ----------

# MAGIC %md
# MAGIC # Adaptive Query Execution (AQE) in Spark
# MAGIC
# MAGIC AQE is especially useful in these scenarios:
# MAGIC
# MAGIC - **Complex queries** with multiple joins, aggregations, or window functions
# MAGIC - **Data skew** situations
# MAGIC - **Large datasets** with imbalanced partition sizes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Switch AQE on/off anytime:**
# MAGIC python
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Further Reading:**
# MAGIC - [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
# MAGIC - [Top 10 Spark Optimization Approaches](https://medium.com/@manoj.kdas37/how-to-optimize-your-apache-spark-jobs-top-10-approaches-and-best-practices-for-performance-tuning-4630ae864f52)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Task: Taxi Dataset Cleaning
# MAGIC
# MAGIC Perform the following data cleaning operations on the taxi dataset:
# MAGIC
# MAGIC - **Vendor Name Validation:** Exclude records where `vendor_name` is null.
# MAGIC - **Passenger Count Filtering:** Remove outliers in the `Passenger_Count` field. Only allow values between 1 and 10 (inclusive). Ensure the column is of integer type.
# MAGIC - **Datetime Casting:** Convert all datetime fields to the `timestamp` data type.
# MAGIC - **Geolocation Precision:** Round `Longitude` and `Latitude` fields to 4 decimal places to prevent downstream issues.
# MAGIC - **Payment Type Normalization:** Standardize the `Payment_type` field to a consistent case (e.g., all lowercase).
# MAGIC - **Duplicate Removal:** Eliminate duplicate rows to ensure data integrity.

# COMMAND ----------



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
