# Databricks notebook source
# MAGIC %md
# MAGIC #### Execute this cell when running locally

# COMMAND ----------

vFileName = "EA Projects Dashboard.xlsx"
vMountDirectory = "/EA-PROJECT-DRF2/"

# COMMAND ----------

vFileName = dbutils.widgets.get("vFileName") 
vMountDirectory = dbutils.widgets.get("vMountDirectory")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Check the file exists in the storage container

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/EA-PROJECT-DRF2")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Project data into a dataframe

# COMMAND ----------

fiscal_year = "2020-21"

projectsDf = spark.read.format("com.crealytics.spark.excel").option("sheetName", fiscal_year + " Projects").option("dataAddress", "A5").option("Header", False).load("dbfs:/mnt" + vMountDirectory + vFileName)


projects_clean_Df = projectsDf.dropna(how="all")
display(projectsDf)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Specialiazation data into a dataframe

# COMMAND ----------


#spzDf = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "NRCanExpertise!A1").option("Header", True).load("dbfs:/mnt" + vMountDirectory + "Specializations_with_Category.csv")

spzDf = spark.read.format("csv").option("header",True).load("dbfs:/mnt" + vMountDirectory + "Specializations_with_Category.csv")

display(spzDf)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ####Provide meaningful column names

# COMMAND ----------

from pyspark.sql.functions import col,lit

projectsDf = projects_clean_Df.withColumnRenamed("_c0","LAST_UPDATED_DT") \
        .withColumnRenamed("_c1","EAD_FILE_NBR") \
        .withColumnRenamed("_c2","PROJECT_NM") \
        .withColumnRenamed("_c3","PROPONENT_LBL") \
        .withColumnRenamed("_c4","PROJECT_TYPE_LBL") \
        .withColumnRenamed("_c5","PROVINCE_LBL") \
        .withColumnRenamed("_c6","EAD_OFFICER_LBL") \
		.withColumnRenamed("_c7","PROCESS_LBL") \
        .withColumnRenamed("_c8","CURRENT STATUS_LBL") \
        .withColumnRenamed("_c9","PHASE_LBL") \
        .withColumnRenamed("_c10","NEXT_DEADLINE_LBL") \
        .withColumnRenamed("_c11","NEXT_ACTIVITY_DESC") \
        .withColumnRenamed("_c12","Expert_Input_Required_For_Next_Deadline_LBL") \
        .withColumnRenamed("_c13","EA_IA_DOCUMENTATION_ISSUED_LBL") \
		.withColumnRenamed("_c14","INPUT_CAPTURED_IN_EA_IA_DOCUMENTATION_LBL") \
        .withColumnRenamed("_c15","ASKED_FOR_HRF_INPUT_LBL") \
		.withColumnRenamed("_c16","HRF_INPUT_PROVIDED_LBL") \
        .withColumnRenamed("_c17","HYDROGEO_IND") \
        .withColumnRenamed("_c18","SEISMICITY_IND") \
        .withColumnRenamed("_c19","TERRAIN_HAZARDS_IND") \
        .withColumnRenamed("_c20","GEOLOGY_IND") \
        .withColumnRenamed("_c21","PERMAFROST_IND") \
        .withColumnRenamed("_c22","COASTAL_GEOSCIENCE_IND") \
        .withColumnRenamed("_c23","MARINE_GEOHAZARDS_IND") \
        .withColumnRenamed("_c24","EXPLOSIVES_IND") \
        .withColumnRenamed("_c25","GEOCHEM_ARD_ML_IND") \
        .withColumnRenamed("_c26","PIPELINE_INT_IND") \
        .withColumnRenamed("_c27","LMS_ECONOMIC_IND") \
        .withColumnRenamed("_c28","FORESTS_FORESTRY_SCIENCE_IND") \
		.withColumnRenamed("_c29","Heavy_Oil_Bitumen_Tailings_IND") \
        .withColumnRenamed("_c30","EMISSIONS_STORAGE_FLARING_HYDROCARBONS_IND") \
        .withColumnRenamed("_c31","ENERGY_ECONOMIC_IND") \
        .withColumnRenamed("_c32","ENERGY_POLICY_IND")  \
        .withColumn("FISCAL_YEAR",lit(fiscal_year))

# COMMAND ----------

projectsDf.groupBy("EA_IA_DOCUMENTATION_ISSUED_LBL").count().show()

# COMMAND ----------

display(projectsDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Add a new column with Province labels and fiscal year

# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.functions import monotonically_increasing_id

projectsDf_with_new_prov_column = projectsDf.withColumn("PROVINCE_DESC", 
                                                        when((projectsDf["PROVINCE_LBL"] == "MB") | (projectsDf["PROVINCE_LBL"] == "MA"), "Manitoba")                                   
                                                       .when(projectsDf["PROVINCE_LBL"] == "ON", "Ontario")
                                                       .when(projectsDf["PROVINCE_LBL"] == 'QC', 'Quebec')
                                                       .when(projectsDf["PROVINCE_LBL"] == 'AB', 'Alberta')
                                                       .when(projectsDf["PROVINCE_LBL"] == 'BC', 'British Columbia')
                                                       .when(projectsDf["PROVINCE_LBL"] == 'NL', 'Newfoundland and Labrador')
                                                       .when(projectsDf["PROVINCE_LBL"] == 'YK', 'Yukon')
                                                       .when(projectsDf["PROVINCE_LBL"] == 'SK', 'Saskatchewan')
                                                       .when(projectsDf["PROVINCE_LBL"] == 'NS', 'Nova Scotia')
                                                       .when(projectsDf["PROVINCE_LBL"] == 'NU', 'Nunavut'))





# COMMAND ----------

# MAGIC %md
# MAGIC ####Add a new column with labels for Next Expected Deadline

# COMMAND ----------

projectsDf_with_new_etd = projectsDf_with_new_prov_column.withColumn("NEXT_DEADLINE_DESC", 
                                                        when((projectsDf_with_new_prov_column["NEXT_DEADLINE_LBL"] == "1"), "Within 15 days")
                                                       .when(projectsDf_with_new_prov_column["NEXT_DEADLINE_LBL"] == "2", "15 - 60 days")
                                                       .when(projectsDf_with_new_prov_column["NEXT_DEADLINE_LBL"] == '3', '60 - 180 days')
                                                       .when(projectsDf_with_new_prov_column["NEXT_DEADLINE_LBL"] == '4', '180 - 360 days')
                                                       .when(projectsDf_with_new_prov_column["NEXT_DEADLINE_LBL"] == '5', '+1 year')
                                                       .when(projectsDf_with_new_prov_column["NEXT_DEADLINE_LBL"] == '6', 'No Deadline'))

display(projectsDf_with_new_etd)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Add a unique id to each row

# COMMAND ----------

projectsDf_with_uniqueID  = projectsDf_with_new_etd.withColumn("PROJECT_ID", monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Unpivot the specialization data from rows to columns

# COMMAND ----------

spunpivotdf=projectsDf_with_uniqueID.selectExpr("PROJECT_ID","PROJECT_NM", "stack(16,'HYDROGEO_IND',HYDROGEO_IND,\
'SEISMICITY_IND',SEISMICITY_IND,\
'TERRAIN_HAZARDS_IND',TERRAIN_HAZARDS_IND,\
'GEOLOGY_IND',GEOLOGY_IND, \
'PERMAFROST_IND',PERMAFROST_IND, \
'COASTAL_GEOSCIENCE_IND',COASTAL_GEOSCIENCE_IND, \
'MARINE_GEOHAZARDS_IND',MARINE_GEOHAZARDS_IND, \
'EXPLOSIVES_IND',EXPLOSIVES_IND, \
'GEOCHEM_ARD_ML_IND',GEOCHEM_ARD_ML_IND, \
'PIPELINE_INT_IND',PIPELINE_INT_IND,\
'LMS_ECONOMIC_IND',LMS_ECONOMIC_IND, \
'FORESTS_FORESTRY_SCIENCE_IND',FORESTS_FORESTRY_SCIENCE_IND,\
'HEAVY_OIL_BITUMEN_TAILINGS_IND',HEAVY_OIL_BITUMEN_TAILINGS_IND,\
'EMISSIONS_STORAGE_FLARING_HYDROCARBONS_IND',EMISSIONS_STORAGE_FLARING_HYDROCARBONS_IND,\
'ENERGY_ECONOMIC_IND',ENERGY_ECONOMIC_IND,\
'ENERGY_POLICY_IND',ENERGY_POLICY_IND) as (SPECIALIZATION_CD, SPECIALIZATION_CNT)").where("SPECIALIZATION_CNT is not null")

spunpivotdf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join the Project dataframe with the Specialization dataframe

# COMMAND ----------

spezfactdf = spunpivotdf.join(spzDf, on=['Specialization_CD'], how='outer')
spezfactdf = spezfactdf.select(["Project_ID","Specialization_CD"])
spezfactdf.count()

# COMMAND ----------

display(spezfactdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write data to the database

# COMMAND ----------

server_name = dbutils.secrets.get(scope = "datalake-key-dev", key = "drf2-sql-server-server")
database_name = dbutils.secrets.get(scope = "datalake-key-dev", key = "drf2-sql-server-db")
url = server_name + ";" + "databaseName=" + database_name + ";"

username = dbutils.secrets.get(scope = "datalake-key-dev", key = "drf2-sql-server-username")   
password = dbutils.secrets.get(scope = "datalake-key-dev", key = "sqladmin-pswd-ciosb-datahub")


#EA_PROJECT_TRACKER 
table_name_project_tracker = "EA_PROJECT_TRACKER"
projectsDf_with_uniqueID.write.format("jdbc").mode("overwrite").option("truncate",True).option("url", url).option("dbtable", table_name_project_tracker).option("user", username) .option("password", password).save()


#SPECIALIZATION
table_name_project_tracker = "EA_SPECIALIZATION"
spzDf.write.format("jdbc").mode("overwrite").option("truncate",True).option("url", url).option("dbtable", table_name_project_tracker).option("user", username) .option("password", password).save()


#EA_PROJECT_SPECIALIZATION 
table_name_specialization = "EA_PROJECT_SPECIALIZATION"
spezfactdf.write.format("jdbc").option("truncate",True).mode("overwrite").option("url", url).option("dbtable", table_name_specialization).option("user", username) .option("password", password).save()





# COMMAND ----------

jdbcDF = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "EA_PROJECT_SPECIALIZATION") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

jdbcDF.select("*").show()