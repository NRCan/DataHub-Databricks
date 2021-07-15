# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE citsm.g_elsevier_affiliation_indicator
# MAGIC (
# MAGIC AFFILIATION_ID INT,
# MAGIC INDICATOR_CD STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/dev/s_ELSEVIER_AFFILIATION_INDICATOR';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO citsm.g_elsevier_affiliation_indicator
# MAGIC values(
# MAGIC 60031860, 'DRF3'
# MAGIC )
