# Databricks notebook source
# MAGIC %md
# MAGIC ###Databricks intro
# MAGIC #<img src="https://img.stackshare.io/service/10345/ADB.png"/></a> Workshop - Databricks (Spark)#
# MAGIC **Today Content**
# MAGIC * Recap
# MAGIC   * Spark
# MAGIC   * Modern storage formats (Delta/Iceberg/Hudi)
# MAGIC   * Lakehouse architecture
# MAGIC   * What is Databricks?
# MAGIC * Basic intro to Databricks
# MAGIC   * Getting know the Databricks platform (UI) and cells
# MAGIC   * Main components in the Free edition
# MAGIC   * Workspaces - Features
# MAGIC   * Look and feel 
# MAGIC   * Cell activites
# MAGIC   * File interaction
# MAGIC  * Other Data Platforms 
# MAGIC  * Links
# MAGIC  * Q&A
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check the account! Free version will be used in the lab
# MAGIC ##https://login.databricks.com/
# MAGIC
# MAGIC
# MAGIC *Beware! MFA authentication! Use valid email address.*
# MAGIC  
# MAGIC  *legacy community link (sundown in 2025): https://community.cloud.databricks.com/login.html*

# COMMAND ----------

# MAGIC %md
# MAGIC ###Recap
# MAGIC - **What is Spark?**
# MAGIC
# MAGIC
# MAGIC *"Apache Spark is an open-source, distributed processing system used for big data workloads. It utilizes in-memory caching and optimized query execution for fast queries against data of any size. Simply put, Spark is a fast and general engine for large-scale data processing."*
# MAGIC
# MAGIC - **Why is Spark?**
# MAGIC
# MAGIC **Use cases for Apache Spark**
# MAGIC - Read and process huge files and data sets
# MAGIC - Query, explore, and visualize data sets
# MAGIC - Join disparate data sets found in data lakes
# MAGIC - Train and evaluate machine learning models
# MAGIC - Process live streams of data
# MAGIC - Perform analysis on large graph data sets and social networks
# MAGIC
# MAGIC **How it works**
# MAGIC - Architecture
# MAGIC #<img src="https://dwgeek.com/wp-content/uploads/2018/07/Apache-Spark-Architecture.jpg"/></a> #
# MAGIC
# MAGIC - Jobs, Tasks, Stages
# MAGIC #<img src="https://miro.medium.com/max/1400/1*Ce0ZFSrXXhR4XrKlRodZUg.png"/></a> #
# MAGIC
# MAGIC **Related buzzwords:** 
# MAGIC *Catalyst, DAG (Logical Plan), Physical Plan, Parallel/Distributed processing, Shuffle, Scheduling Methods, Cluster, Driver, Executor, Cores (Slot), Partitions (parallelism), ScaleUp/ScaleOut*
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Running Spark Cluster
# MAGIC
# MAGIC <img src="https://miro.medium.com/v2/resize:fit:720/format:webp/0*l-bMPfkbjfkJJOZU"/></a>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dataframes
# MAGIC #
# MAGIC * The DataFrame API is the most common Structured API and simply represents a table of data with rows and columns
# MAGIC * Conceptually equivalent to a table in a relational database or a data frame in R/Python
# MAGIC * Based on RDD (Resilient Distributed Dataset)
# MAGIC * The Apache Spark DataFrame API provides a rich set of functions (select columns, filter, join, aggregate, and so on) that allow you to solve common data analysis problems efficiently
# MAGIC
# MAGIC More info:
# MAGIC
# MAGIC <a href="https://databricks.com/glossary/what-are-dataframes" target="_blank">Dataframes</a>
# MAGIC
# MAGIC <a href="https://docs.databricks.com/getting-started/spark/dataframes.html?_ga=2.247437638.399052973.1615643506-581116704.1614197786" target="_blank">DataFrame intro</a>
# MAGIC
# MAGIC <a href="https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html" target="_blank">RDD vs Dataframe</a>
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Dataframes vs Dataset?
# https://www.databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dataframe operation types
# MAGIC #
# MAGIC
# MAGIC The dataframe processing (key concept in Spark) is the `lazy evaluation`. What does it mean?
# MAGIC The operations have two main groups: 
# MAGIC  - `Transformation`:
# MAGIC    * They eventually return another DataFrame.
# MAGIC    * They are immutable - that is each instance of a DataFrame cannot be altered once it's instantiated.
# MAGIC    * Are classified as either a Wide or Narrow operation
# MAGIC  - `Action`:
# MAGIC    * An action provide the ability to return previews or specify physical execution plans for how logic will map to data. 
# MAGIC    * In other words: Actions are commands that are computed by Spark right at the time of their execution. 
# MAGIC    * They consist of running all of the previous transformations in order to get back an actual result. 
# MAGIC    * An action is composed of one or more jobs which consists of tasks that will be executed by the workers in parallel where possible
# MAGIC    
# MAGIC  - A `wide` transformation requires sharing data across workers (e.g.: join, groupby, dropDuplicate) -> need to shuffle
# MAGIC  - A `narrow` transformation can be applied per partition/worker with no need to share or shuffle data to other workers (e.g.: select, where, union, etc.)
# MAGIC  
# MAGIC  
# MAGIC Examples:
# MAGIC <img src="https://www.rakirahman.me/static/a0cbe1310a5fa370445da458d8aa8b30/d15a3/actions-overview.png"/></a>
# MAGIC <br>
# MAGIC *Can you categorize the repartition and coalesce?*

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pyspark Dataframes vs Pandas Dataframes
# MAGIC #
# MAGIC * Main differences:
# MAGIC    * Evaluation (Lazy vs Eager)
# MAGIC    * Opearational environment (Cluster vs Single)
# MAGIC    * Muatability (Immutable vs Mutable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Big Data file formats:
# MAGIC
# MAGIC <img src="https://miro.medium.com/v2/resize:fit:871/1*Feyzf6lootJmBB8-77jXdw.png" width="400" height="400"/></a>
# MAGIC <br>
# MAGIC *Columnar vs Row type, Read vs Write, Compression, Distributed/Partition/Bucket, Small file problem, Self-describing, Predicate/Projection pushdown, Partition pruning,*
# MAGIC
# MAGIC ## Transactional Storage Frameworks:
# MAGIC <img src="https://miro.medium.com/v2/resize:fit:720/format:webp/1*vdOqqDFY_RlPMy-6rKxUbg.png" width="400" height="400"/></a> 
# MAGIC <br>
# MAGIC *ACID, Schema evolution, TimeTravel, Partition evolution, Performance*
# MAGIC
# MAGIC *Delta is the native default format for DBX*
# MAGIC
# MAGIC *Iceberg only supported on classic compute environment on DBX*

# COMMAND ----------

# MAGIC %md
# MAGIC <h2 style="text-align: center;">Analytics platform evolution</h2>
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2020/01/data-lakehouse.png" alt="LakeHouse" style="width: 600px height: 600px" >
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <h2 style="text-align: center;">Lakehouse - Medallion architecture</h2> 
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" alt="LakeHouse" style="width: 600px height: 600px">
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lakehouse supports
# MAGIC #
# MAGIC * Transaction support
# MAGIC * Schema enforcement and governance
# MAGIC * BI support
# MAGIC * Storage is decoupled from compute
# MAGIC * Openness
# MAGIC * Support for diverse data types ranging from unstructured to structured data
# MAGIC * Support for diverse workloads
# MAGIC * End-to-end streaming
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##What is Databricks?
# MAGIC
# MAGIC Old:
# MAGIC
# MAGIC *"Databricks is a fully-managed version of the open-source Apache Spark analytics and data processing engine. Databricks is an enterprise-grade and secure cloud-based big data processing and machine learning platform.*
# MAGIC *Databricks provides a notebook-oriented Apache Spark as-a-service workspace environment, making it easy to manage clusters and explore data interactively."* --> till last year
# MAGIC
# MAGIC New:
# MAGIC
# MAGIC *"Databricks is a unified, open analytics platform for building, deploying, sharing, and maintaining enterprise-grade data, analytics, and AI solutions at scale. The <b>Databricks Data Intelligence Platform</b> integrates with cloud storage and security in your cloud account, and manages and deploys cloud infrastructure for you."*
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##What does Databricks offer?
# MAGIC
# MAGIC * Databricks SQL (Warehousing)
# MAGIC * Orchestration/Pipelines - Lakeflow Pipelines (DLT)
# MAGIC * AI/BI  (Genie)
# MAGIC * AI/ML - Mosaic AI
# MAGIC * Data Governance - Unity Catalog
# MAGIC * Databricks Assistant (LLM Chatbot)
# MAGIC * Managed Compute (Spark/Warehouse) environment

# COMMAND ----------

# MAGIC %md
# MAGIC ###Databricks architecture
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="https://docs.databricks.com/aws/en/assets/images/data_intelligence_engine-5293849e711ddffe8e6e86418e36ae31.png" width="600" height="500"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Supported multiple languages:
# MAGIC
# MAGIC
# MAGIC * **Scala** - Apache Spark's primary language
# MAGIC * **Python** - More commonly referred to as PySpark
# MAGIC * **R** - SparkR (R on Spark)
# MAGIC * **Java** - (Not directly -- need hack)
# MAGIC * **SQL** - Closer to ANSI SQL 2003 compliance
# MAGIC
# MAGIC *Remark: With the DataFrames API, the performance differences between languages are nearly nonexistence (especially for Scala, Java & Python).*
# MAGIC
# MAGIC *On Serverless compute only Pyspark and SQL are available*

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setting up the environment - Get our hands dirty!

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Compute** <img src="https://docs.microsoft.com/en-gb/azure/databricks/_static/images/icons/clusters-icon.png"/></a> - Serverless vs Classic
# MAGIC * To able to run anything first we need create a cluster (or find an existing and start it)
# MAGIC * In the Free edition only the Serverless is available
# MAGIC
# MAGIC ### Key Considerations When Choosing
# MAGIC * Simplicity vs. Control: Do you need a hands-off, fully managed experience or detailed control over your compute environment? 
# MAGIC * Cost Management: Are you running short, frequent jobs (where Serverless might be cheaper) or long, stable jobs (where Classic might be better)? 
# MAGIC * Workload Type: Serverless is great for simplicity and speed, while Classic is better for workloads that need extensive customization. 
# MAGIC * Technical Expertise: Serverless requires less infrastructure management expertise, whereas Classic benefits from users who want to manage their own resources. 
# MAGIC
# MAGIC *The serverless cluster has more limitation!*

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://miro.medium.com/v2/resize:fit:720/format:webp/0*vqLeBl0uPlrwhTSX"" width="1200" height="600"/></a> 
# MAGIC  
# MAGIC  *Classic compute configs*
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## About Notebooks as development environment
# MAGIC   **What is a notebook?**
# MAGIC
# MAGIC *"A notebook is a web-based interface to a document that contains runnable code, visualizations, and narrative text."*
# MAGIC
# MAGIC **Main alternatives:**
# MAGIC
# MAGIC * Jupyter Notebook <img src="https://logodix.com/logo/1741522.png" width="50" height="50"/></a> 
# MAGIC * Google Colab <img src="https://colab.research.google.com/img/colab_favicon_256px.png" width="50" height="50"/></a>  
# MAGIC * Apache Zeppelin <img src="https://pbs.twimg.com/profile_images/1091493522501337088/uT9CEBb0_400x400.jpg" width="50" height="50"/></a> 
# MAGIC * MS Fabric Notebook <img src="https://msfabric.pl/wp-content/uploads/2022/02/FAVI.png" width="50" height="50"/></a> 
# MAGIC
# MAGIC
# MAGIC *Also available Notbeook support in the main local IDEs such PyCharm, VSCode, etc...*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Notebook
# MAGIC
# MAGIC 1. In the sidebar, use the **New** (+) button.
# MAGIC 1. On the pane page, click **Notebook**.
# MAGIC 1. Name the Notebook.
# MAGIC 1. Choose the Python language (default).
# MAGIC 1. Select the running cluster (if exist).
# MAGIC 1. Click **Create**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## UI check - How to run the code?
# MAGIC * Each notebook can be tied to a specific language: **Scala**, **Python**, **SQL** , **R** (or **MD** ) *
# MAGIC * Run the cell below using one of the following options:
# MAGIC   * **CTRL+ENTER** or **CMD+RETURN**
# MAGIC   * **SHIFT+ENTER** or **SHIFT+RETURN** to run the cell and move to the next one
# MAGIC   * Using **Run Cell**, **Run All Above** or **Run All Below** as seen here/>
# MAGIC
# MAGIC   *Serverless does not support R and Scala*

# COMMAND ----------

#Challenge
#TODO
# print something
print("Hello DBX!")


# COMMAND ----------

# DBTITLE 0,Playing around
#Challenge
#TODO
# some calculation
x = 1 + 3
print(f" variable: {x}")


# COMMAND ----------

#Challenge
#TODO
# assign some variables
y = 12
z = 'monkeys'

# COMMAND ----------

#TOD
# Check the variables values
print(str(y) + ' ' + z) 

# COMMAND ----------

# DBTITLE 1,Experiment Jobs/stages
#Challenge
#TODO
import pyspark.sql.functions as F

# Create dataframe! Choose any solution
# Analyze the jobs and outputs (Refer the Spark execution!) --> no sign in Serverless
# Check How many jobs/stages created? --> no sign in Serverless
# Check the Performance of menu.
# Check the Visulaization menu.
# Check the display funcionalities! Discuss about the display built-in Databricks function!
# Check Data Assistant capabilities!
#
df_schema=['city', 'population', 'k_index']
db_list = [('Megacity', 30_002_000, 21), ('Metropolis',15_000_111, 31)]
print(db_list)

df = spark.createDataFrame(db_list, schema=df_schema)
df.display()
#display(df)

# COMMAND ----------

#Comment
#That's simple isn't it??👍🚀🚀🚀🚀☝️

# COMMAND ----------

# MAGIC %md
# MAGIC ##Magic Commands
# MAGIC * Magic Commands are specific to the Databricks notebooks
# MAGIC * They are very similar to Magic Commands found in comparable notebook products (language agnostic capability)
# MAGIC * These are built-in commands that do not apply to the notebook's default language
# MAGIC * A single percent (%) symbol at the start of a cell identifies a Magic Commands
# MAGIC * E.g.: `scala`, `r`, `python`, `sql`, `sh`, `run`, `md`, etc.. 

# COMMAND ----------

# MAGIC %sh 
# MAGIC echo 'Hello'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'Hello SQL!'

# COMMAND ----------

# MAGIC %md
# MAGIC ##MarkDown Cell 
# MAGIC # 
# MAGIC   - Help to organize the code and data other stuffs
# MAGIC   - Or make more mathematical: \\(c = \\sqrt{a^2 + b^2} \\)
# MAGIC   ---
# MAGIC   - More info: <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">MarkDown CheatSheet</a>.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unity Catalog
# MAGIC
# MAGIC * Centralized Data Repository
# MAGIC * Governancae and security
# MAGIC * Lineage and discovery
# MAGIC * Manage the file interacions (local and remote) (Local: DBFS - distbributed file system for Databricks )
# MAGIC
# MAGIC
# MAGIC
# MAGIC More info:
# MAGIC
# MAGIC <a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/" target="_blank">Unity Catalog</a>
# MAGIC <a href="https://docs.databricks.com/en/dbfs/index.html" target="_blank">Databricks File System - DBFS</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # File Interaction

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/songs/data-001/')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /Volumes/workspace/default/user/
# MAGIC

# COMMAND ----------

ls /Volumes/workspace/default/user/test.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC head /Volumes/workspace/default/user/test.csv
# MAGIC
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/user/"))


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default

# COMMAND ----------

# DBTITLE 1,How to upload data?
# File --> Upload data to Volume (DBFS)
# Grab some data and try to load it and create dataframe!
# Example: https://freetestdata.com/other-files/csv/
# Check Catalog Explorer
# If we have time check the different csv load types

# COMMAND ----------

#Challenge
#TODO
csv_path = "/Volumes/workspace/default/user/test.csv"
csv_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)
display(csv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##dbutils
# MAGIC * Databricks utility - command collections for variety tasks. E.g.: file system operations
# MAGIC
# MAGIC https://docs.databricks.com/dev-tools/databricks-utils.html

# COMMAND ----------

# MAGIC %md
# MAGIC * Other useful dbutils command:
# MAGIC `dbutils.help()`

# COMMAND ----------

# MAGIC %md
# MAGIC **HW:**
# MAGIC - Task - Practice how to use docs
# MAGIC   - _https://docs.databricks.com/dev-tools/databricks-utils.html_
# MAGIC
# MAGIC Example steps:
# MAGIC   - Create own folder(s)
# MAGIC   - Execute some fs opearations 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2 style="text-align: center;"><a href="https://docs.databricks.com/" target="_blank">🤘☝️☝️Databricks Documentations☝️☝️🤘</a></h2>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌐 Other Data Platforms
# MAGIC
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 20px; justify-content: center; align-items: center;">
# MAGIC
# MAGIC   <div style="text-align: center;">
# MAGIC     <img src="https://miro.medium.com/v2/resize:fit:654/1*m1yj2VxL-Rp3aqkCzJEdVg.png" width="250" height="250"/>
# MAGIC     <div style="font-size: 16px; margin-top: 8px;">AWS Glue</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="text-align: center;">
# MAGIC     <img src="https://www.optimusinfo.com/wp-content/uploads/2020/09/synapse-case-case.png" width="250" height="250"/>
# MAGIC     <div style="font-size: 16px; margin-top: 8px;">Azure Synapse</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="text-align: center;">
# MAGIC     <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTGtsjtT26xLbvGO_eRAcJJ2drgv6wC9S7REQ&s" width="250" height="250"/>
# MAGIC     <div style="font-size: 16px; margin-top: 8px;">Snowflake</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="text-align: center;">
# MAGIC     <img src="https://cyprusshippingnews.com/wp-content/uploads/2024/09/Palantir-Logo-New.jpg" width="250" height="250"/>
# MAGIC     <div style="font-size: 16px; margin-top: 8px;">Palantir</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="text-align: center;">
# MAGIC     <img src="https://cdn.plainconcepts.com/wp-content/uploads/2024/09/Microsoft-Fabric-and-Telcos.png" width="250" height="250"/>
# MAGIC     <div style="font-size: 16px; margin-top: 8px;">Microsoft Fabric</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC <h2 style="text-align: center;">❓❓❓ Q & A  ✔️✔️✔️</a>
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ##More Databrikcs Stuff
# MAGIC    * <a href="https://www.databricks.com/product/databricks-sql" target="_blank">Databricks Warehouse (BI) Components</a>
# MAGIC    * <a href="https://www.databricks.com/blog/2022/05/10/introducing-databricks-workflows.html">Databricks ETL processes</a>
# MAGIC    * <a href="https://www.databricks.com/blog/2023/03/24/hello-dolly-democratizing-magic-chatgpt-open-models.html">Hello Dolly (ChatGPT)</a>
# MAGIC    * <a href="https://docs.databricks.com/en/large-language-models/index.html">LLM on Databricks</a>
# MAGIC
# MAGIC    ---
# MAGIC ##Other General Stuff   
# MAGIC    * <a href="https://towardsdatascience.com/dbt-55b35c974533">DBT - cool data transformation tool</a>
# MAGIC    * <a href="https://www.datamesh-architecture.com/">Data Mesh concept</a>
# MAGIC

# COMMAND ----------

# DBTITLE 1,Language agnostic
# %scala --> cannot test it on serverless
# //Create some variable
# val n = 5
