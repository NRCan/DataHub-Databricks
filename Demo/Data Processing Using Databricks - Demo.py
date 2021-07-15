# Databricks notebook source
# MAGIC %md
# MAGIC ##Databricks Overview (Demo)
# MAGIC ####. Intro to Apache Spark and Databricks
# MAGIC ####. Databricks Clusters (Compute)
# MAGIC ####. Access to Data Lake (Storage)
# MAGIC ####. Data Preparation and Analysis using Spark SQL (Python, R and SQL)
# MAGIC ####. Integration with RStudio (Open source version)
# MAGIC ####. Persistently store the cleansed dataset in the data lake

# COMMAND ----------











# COMMAND ----------

# MAGIC %md
# MAGIC <img src ='/files/Spark_Cluster.JPG'>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src ='/files/Spark_Eco_System.JPG'>

# COMMAND ----------





# COMMAND ----------

# MAGIC %md 
# MAGIC #Easily store and access data from the Data Hub (Data Lake)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Access files from Data Lake storage and load it into a spark dataframe using Python

# COMMAND ----------

df=spark.read.format('csv').option("header","true").load('abfss://databricks-demo@datahubdatalakedev.dfs.core.windows.net/demo/export-2.csv')

# COMMAND ----------

# MAGIC %md 
# MAGIC #Data manipulation using Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display data from the dataframe

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select specific columns

# COMMAND ----------

df.select("GEO","Value","Date").where("value >6000").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter out rows 

# COMMAND ----------

dfND = df.where(df["VALUE"].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregate/Count by Column

# COMMAND ----------

dfCountByLF=dfND.groupBy("Labour_force_characteristics").count()
display(dfCountByLF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter for records with Full Time Employment as labour force characteristic

# COMMAND ----------

dfFullTimeEmp = dfND.filter('Labour_force_characteristics=="Full-time employment"')
display(dfFullTimeEmp)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Use Pandas and Spark Dataframe in the same notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Spark Dataframe to regular Python dataframe if needed (Pandas)

# COMMAND ----------

pandadf = dfFullTimeEmp.select("*").toPandas()

# COMMAND ----------

pandadf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Pandas dataframe from scratch

# COMMAND ----------

import pandas as pd
data = [1,2,3,4,5]
pandadf = pd.DataFrame(data)
print(pandadf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Pandas dataframe to Spark Dataframe
# MAGIC ######*Not all data types are supported

# COMMAND ----------

sparkdf = spark.createDataFrame(pandadf)

# COMMAND ----------

# MAGIC %md
# MAGIC #Use Standard SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Dataframe to Temporary Table to analyze data using SQL

# COMMAND ----------

dfFullTimeEmp.createOrReplaceTempView("vw_dfFullTimeEmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_dfFullTimeEmp 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(value) Employment, NAICS FROM vw_dfFullTimeEmp 
# MAGIC WHERE NAICS <> 'Total, all industries'
# MAGIC GROUP BY NAICS 
# MAGIC ORDER BY Employment desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Create User Defined Functions in Python (or Scala, R) and use it in SQL

# COMMAND ----------

def converted_value(pval: float):
  return pval * 1.5
spark.udf.register("alteredvalue", converted_value, FloatType())

# COMMAND ----------

# MAGIC %sql select NAICS,converted_value(cast(VALUE as double)) as new_value from vw_dfFullTimeEmp

# COMMAND ----------

# MAGIC %md
# MAGIC # Create persistent table in the Data Lake 
# MAGIC #####. Data is stored in columnar (Parquet) format 
# MAGIC #####. Can be queried and updated by multiple users (ACID compliance)
# MAGIC #####. Versioned
# MAGIC #####. Accessible via JDBC (Power BI, Tableau etc)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DEMO_LABOUR_FORCE 
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://databricks-demo@datahubdatalakedev.dfs.core.windows.net/demo/tbl/'
# MAGIC AS SELECT * FROM vw_dfFullTimeEmp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE DEMO_LABOUR_FORCE SET Labour_force_characteristics = 'Full Time'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DEMO_LABOUR_FORCE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DEMO_LABOUR_FORCE TIMESTAMP AS OF '2021-02-22T22:32:33.000+0000'

# COMMAND ----------

# MAGIC %md
# MAGIC # Using R in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Integration of RStudio in Databricks (Open Source Version)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert data to SparkR dataframe

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC 
# MAGIC sdrf2<-sql("SELECT * FROM DEMO_LABOUR_FORCE")

# COMMAND ----------

# MAGIC %r
# MAGIC showDF(sdrf2)
