# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.g_ACKNOWLEDGEMENTS_BY_YEAR (
# MAGIC YEAR_LBL string,
# MAGIC ACKNOWLEDGEMENTS string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/dev/g_ACKNOWLEDGEMENTS_BY_YEAR';

# COMMAND ----------

abydf=spark.read.format('csv').option("header","true").load('/mnt/gold/dev/Acknowledgements_By_Year.csv')

# COMMAND ----------

abydf.createOrReplaceTempView("abydfvw")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE CITSM.g_ACKNOWLEDGEMENTS_BY_YEAR;
# MAGIC 
# MAGIC INSERT INTO CITSM.g_ACKNOWLEDGEMENTS_BY_YEAR
# MAGIC SELECT * FROM abydfvw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CITSM.g_ACKNOWLEDGEMENTS_BY_YEAR

# COMMAND ----------

