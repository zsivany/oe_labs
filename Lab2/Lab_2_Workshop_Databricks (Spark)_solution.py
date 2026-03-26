# Databricks notebook source
# MAGIC %md
# MAGIC #<img src="https://img.stackshare.io/service/10345/ADB.png"/></a> Workshop Lab 2 - Databricks#
# MAGIC **Content**
# MAGIC   * Spark DataFrame operations
# MAGIC   * Partitions example
# MAGIC   * Delta Parquet (features) - Base of the Lakehouse architecture
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

parquet_df = spark.read.format("parquet").load("/Volumes/workspace/default/user/Lab_2-userdata1.parquet")


# COMMAND ----------

# Challenge
# Check the schema of the datafrme
# TODO
parquet_df.printSchema()

# Display the dataframe to get some pict about the data
# TODO
parquet_df.display()

# Display some selected columns and filter on the dataframe and limit it to 20 rows
# TODO
parquet_df.select('id', 'email', 'gender').filter(parquet_df.id > 100).limit(20).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Some advanced and common dataframe operations

# COMMAND ----------

# Challenge
# Show how many records by country, descreasing order
# Question: What is Finland's ranking in this order? (Not the count value itself)


#TODO
#
parquet_df.select('country').groupBy('country').count().orderBy(F.desc('count')).display()
#Beware of the diff of sort and orderby and sortWithinPartitions

# COMMAND ----------

# aggregate functions (min, max, avg, sum)
# more options: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html

display(parquet_df.agg(
    F.min(parquet_df.id),
    F.max(parquet_df.id),
    F.avg(parquet_df.id),
    F.sum(parquet_df.id)
))

# COMMAND ----------


#Add new columns and check the data types of the new cols
parquet_df.withColumn('simple_string', F.lit("Constant_with_string")).withColumn('date', F.lit(datetime.datetime.now())).withColumn('str_date', F.col('date').cast('string')).printSchema()

# COMMAND ----------

# Challenge
# Playing with Nulls
# .na method
# dropna(subset=cols) --> optional
# fillna(value, col) --> col optional, datatype eqaulity

#TODO
parquet_df.select("salary").display()
parquet_df.select("gender").display()
parquet_df.na.replace("", None).dropna(subset="gender").display()
parquet_df.na.replace("", None).fillna(0, 'salary').select('salary').display()


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

number_of_rows = parquet_df.count()
number_distinct_rows = parquet_df.dropDuplicates().count()
number_distinct_country_rows = parquet_df.dropDuplicates(['country']).count()
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
#select the user from Finland articles 
country_list = parquet_df.where(F.col('country') == F.lit('Finland')).collect()

type(country_list)
country_list

# Row object element, why is more/better than simple list? Make a loop for seeing the difference:
# TODO
for i in range(len(country_list)):
  print(country_list[i].email)
  print(country_list[i].title)


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

# The joins are very similar as in the SQL and exists all well known mode: innner, left outer, full outer, right outer, anti, etc.
joined_df = empDF.join(deptDF, empDF.dept_id ==  deptDF.dept_id, how="left")
display(joined_df)

# HW
# try with another join types


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
#original range: 10000000
from pyspark.sql.functions import *

big_df = spark.range(1_000_000_000).toDF("number")

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

#Normal query - without partition column
display(parted_df.where("number = 13759"))

# COMMAND ----------

#Normal query - with partition column
display(parted_df.where("part = 9 and number = 13759"))

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

# Delta parquet / delta / databricks delta
# Write out in delta format the earlier dataframe. Only different is the format and the location
parted_df.write.mode("append").format("delta").partitionBy("part").save(f'/Volumes/{delta_parquet.replace(".", "/")}')

# COMMAND ----------


display(dbutils.fs.ls(f'/Volumes/{delta_parquet.replace(".", "/")}'))

# COMMAND ----------

#Read the delta table - temp view for sql command
delta_df = spark.read.format("delta").load(f'/Volumes/{delta_parquet.replace(".", "/")}')
# For the sql API
delta_df.createOrReplaceTempView("delta_parquet")

# COMMAND ----------

# Challenge
# Lets make some ACID transaction (update, delete)
# There are several way to use --> can use anyone

# COMMAND ----------

#1: Delete
spark.sql("delete from delta_parquet where number = 1600")

# COMMAND ----------

#2: Update
spark.sql("update delta_parquet set number= -1 where number = 1599")


# COMMAND ----------

# Check the records
spark.sql("select * from delta_parquet where number = 1600").display()
spark.sql("select * from delta_parquet where number = -1").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Challenge**
# MAGIC - Check and try the upsert operation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Timetravel function in Delta

# COMMAND ----------

# Check the Timetravel 
spark.sql(f"DESCRIBE HISTORY delta.`/Volumes/{delta_parquet.replace('.', '/')}`").display()

# COMMAND ----------

# Set back to the earlier version
spark.sql(f"RESTORE TABLE delta.`/Volumes/{delta_parquet.replace('.', '/')}` TO VERSION AS OF 0").display()

# COMMAND ----------

# Challenge
# Check the earlier deleted records
spark.sql("select * from delta_parquet where number = 1600").display()

# COMMAND ----------

# Challenge
# Check the earlier updated records
spark.sql("select * from delta_parquet where number = -1").display()

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

# MAGIC %md
# MAGIC Extra cells

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


