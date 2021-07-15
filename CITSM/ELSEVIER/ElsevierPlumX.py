# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Elsevier PlumX Metrics 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implementation Strategy
# MAGIC 
# MAGIC 1) Call the PlumX API itteratively for all the Publications that are currently in the database
# MAGIC   - Use the dc_identifier (scopusId) from the Publication table as the key to link to the PlumX data
# MAGIC   - Ingest and store the json files in the Blob Storage /mnt/bronze/elsevier-json/
# MAGIC   
# MAGIC 2) Parse the json files, perform required transformation, and store it in a table in Delta Lake
# MAGIC 
# MAGIC 3) The ingested PlumX data is stored at the lowest grain, which is the Citation Sources. Overall, there are 3 levels of information in the dataset:
# MAGIC   - Category (Eg: Capture, Mention, Citation, Social Media )
# MAGIC   - Each Category may contain 0 or more Sub-Category (Eg: Citation - > (Reader_Count, Export_Saves), Socal Media ->( Facebook_Count, Tweet_Count), Citation -> (Cited_By_Count))
# MAGIC   - The sub-category Citation,  may have 0 or more Citation Sources (Eg: Citation -> (Scopus, Public Media, Cross Ref))
# MAGIC                               
# MAGIC 
# MAGIC 4) Data Profiling/Validation and Data Anomoly Testing
# MAGIC   - Basis statistics about the PlumX data currently captured
# MAGIC     - Test for null values in the key fields (Category, Count Types etc)
# MAGIC     - Number of Publications for which the PlumX data exists 
# MAGIC     - Number of Publications that did not have PlumX metrics associated 
# MAGIC     - Distinct possible values for Categories, Sub-categories, Citation Source Names
# MAGIC 
# MAGIC 5) Data cleansing and transformations
# MAGIC    - To be implemented based on feedback 
# MAGIC 
# MAGIC 5) Create Delta Lake tables to be accessed by end users
# MAGIC   - g_Denorm_ElsevierPlumX : This table contains the denormalized data at the grain of Citation Source Types
# MAGIC   - g_Category_ElsevierPlumX : This table contains measures at the grain of Sub-Category (Citation, Social Media, etc.) 
# MAGIC   - g_Citation_Sources_ElsevierPlumX : This table contains measures at the Citation Sources
# MAGIC   - if required different can be presented in different structures based on requirements/feedback.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Set the run date manually

# COMMAND ----------

datestring = '2021-07-08'

# COMMAND ----------

# MAGIC %md
# MAGIC ####Set the run date based on the parameter received from ADF. This cell is not to be run when executing this notbeook manually

# COMMAND ----------

datestring = dbutils.widgets.get("datestring")   ##run date gets updated based on the parameter passed from ADF
print(datestring)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parse the json files and insert the records into the a delta table 

# COMMAND ----------

#Call the PlumX API itteratively for all the Publications that are currently in the database

import requests
import json
import io
import glob
from collections import namedtuple

directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/PlumX/"  ##Blob storage location for storing the json files

allIDs = [i.document_id for i in spark.sql("select document_id from  CITSM.s_elsevier_publication").collect()] 

##required API Key and Instoken to access the API
headers = {'X-ELS-APIKey':dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierAPIKey"),'X-ELS-Insttoken': dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierInsttoken")}
x=0

for scopusID in allIDs:  ##iterate through each publication and retrive the json file
    query = "https://api.elsevier.com/analytics/plumx/scopusId/{0}".format(scopusID)
    print ('processing Scopus ID {0} - {1}'.format(scopusID,query))
    response = requests.get(query, headers = headers) #verify false ignores certificate
    fName = 'PlumX-{0}.json'.format(scopusID)
    data = json.loads(response.text)
    dbutils.fs.put(directory+fName, response.text, True)
    x = x + 1
    print ('saved {0}'.format(fName))
    print(x)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare the delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS  CITSM.s_ELSEVIER_PLUMX (
# MAGIC scopusId string,
# MAGIC category string,
# MAGIC subCategory string,
# MAGIC count bigint,
# MAGIC citationSource string,
# MAGIC citationSourceCount string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_PLUMX';
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE  IF NOT EXISTS   CITSM.s_ELSEVIER_PLUMX_METRIC (
# MAGIC DOCUMENT_ID string not null,
# MAGIC CATEGORY_LBL string not null,
# MAGIC SUB_CATEGORY_LBL string not null,
# MAGIC METRIC_CNT bigint
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_PLUMX_METRIC';
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE  IF NOT EXISTS  CITSM.s_ELSEVIER_PLUMX_CITATION_SOURCE (
# MAGIC DOCUMENT_ID string not null,
# MAGIC CATEGORY_LBL string not null,
# MAGIC SUB_CATEGORY_LBL string not null,
# MAGIC CITATION_SOURCE_LBL string not null,
# MAGIC METRIC_CNT bigint 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_PLUMX_CITATION_SOURCE';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  schema1.s_ELSEVIER_PLUMX (
# MAGIC scopusId string,
# MAGIC category string,
# MAGIC subCategory string,
# MAGIC count bigint,
# MAGIC citationSource string,
# MAGIC citationSourceCount string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_PLUMX';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parse the json files and insert the records into the a delta table 

# COMMAND ----------

#Bulk load json files into dataframe and perform the required transformation 
from pyspark.sql.functions import split, explode, explode_outer, col
import re

#location of the json file within the blog storage
directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/PlumX/"    #parametize this

#define the schema using the sample json file
#df=spark.read.json(directory +  "PlumX-85042540105.json")
#df=spark.read.json(directory +  "PlumX-84956759853.json")
df=spark.read.json("/mnt/schema/PlumX-85042540105.json")

schema = df.schema

#load the entire directory of json file into the data frame
testJsonData = sqlContext.read.schema(schema).json(directory)

#perform required transformation by flattening the nested arrays within the json file and store data into a new dataframe
newDf = testJsonData.withColumn("categories",explode("count_categories")) \
                        .withColumnRenamed("id_value","Scopus_ID") \
       .select(col("Scopus_ID"),col("categories.name").alias("Category"),explode("Categories.count_types").alias("count_types")) \
.select(col("Scopus_ID"),col("Category"),col("count_types.name").alias("Count_Type"),col("count_types.total").alias("Count"),explode_outer("count_types.sources").alias("sources")) \
.select(col("Scopus_ID"),col("Category"),col("Count_Type"),col("Count"),col("sources.name").alias("Citation_Sources_Name"),col("sources.total").alias("Citation_Sources_Count"))

newDf.createOrReplaceTempView("PlumXView")

spark.sql("""truncate table CITSM.s_ELSEVIER_PLUMX """)

spark.sql("""insert into CITSM.s_ELSEVIER_PLUMX
                  select 
                    CAST(Scopus_ID as string) as scopusId,
                    Category as category,
                    Count_Type as subCategory,
                    Count as count,
                    Citation_Sources_Name as citationSource,
                    Citation_Sources_Count as citationSourceCount
                  from PlumXView""")


# COMMAND ----------

# MAGIC   %sql
# MAGIC   SELECT scopusID as DOCUMENT_ID,
# MAGIC           category as CATEGORY_LBL,
# MAGIC           subCategory as SUB_CATEGORY_LBL,
# MAGIC           max(count) as METRIC_CNT
# MAGIC   FROM CITSM.s_ELSEVIER_PLUMX
# MAGIC   GROUP BY scopusID,category,subCategory

# COMMAND ----------

# MAGIC %md
# MAGIC #### Populate the PlumX Metric table

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC TRUNCATE TABLE CITSM.s_ELSEVIER_PLUMX_METRIC;
# MAGIC 
# MAGIC INSERT INTO CITSM.s_ELSEVIER_PLUMX_METRIC
# MAGIC (
# MAGIC   SELECT scopusID as DOCUMENT_ID,
# MAGIC           category as CATEGORY_LBL,
# MAGIC           subCategory as SUB_CATEGORY_LBL,
# MAGIC           max(count) as METRIC_CNT
# MAGIC   FROM CITSM.s_ELSEVIER_PLUMX
# MAGIC   GROUP BY scopusID,category,subCategory
# MAGIC   ORDER BY scopusID,category
# MAGIC );
# MAGIC 
# MAGIC OPTIMIZE CITSM.g_ELSEVIER_PLUMX_METRIC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CITSM.s_ELSEVIER_PLUMX_METRIC order by document_id,category_lbl;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Populate the PlumX Citation Source table

# COMMAND ----------

# MAGIC %sql 
# MAGIC TRUNCATE TABLE CITSM.s_ELSEVIER_PLUMX_CITATION_SOURCE;
# MAGIC 
# MAGIC INSERT INTO CITSM.s_ELSEVIER_PLUMX_CITATION_SOURCE
# MAGIC (
# MAGIC   SELECT distinct scopusID as DOCUMENT_ID
# MAGIC           ,category as CATEGORY_LBL
# MAGIC           ,subCategory as SUB_CATEGORY_LBL
# MAGIC           ,citationSource as CITATION_SOURCE_LBL
# MAGIC           ,citationSourceCount as METRIC_CNT
# MAGIC   FROM CITSM.s_ELSEVIER_PLUMX
# MAGIC   WHERE category="citation"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CITSM.s_ELSEVIER_PLUMX_CITATION_SOURCE ORDER BY DOCUMENT_ID,CATEGORY_LBL
