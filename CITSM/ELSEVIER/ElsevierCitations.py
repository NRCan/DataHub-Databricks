# Databricks notebook source
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

# MAGIC %sql
# MAGIC SELECT count(*) from citsm.s_elsevier_publication

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest json files and store them in the blob storage

# COMMAND ----------

#Call the Citation Overview  itteratively for all the Publications that are currently in the database
import requests
import json
import io
import glob
from collections import namedtuple
from datetime import date

directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/citations/"  ##Blob storage location for storing the json files

spark.sql("""optimize CITSM.s_ELSEVIER_PUBLICATION""")

allIDs = [i.document_id for i in spark.sql("select document_id from CITSM.s_ELSEVIER_PUBLICATION").collect()] 
#where document_id=85042540105


##required API Key and Instoken to access the API
headers = {'X-ELS-APIKey':dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierAPIKey"),'X-ELS-Insttoken': dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierInsttoken")}
strEndYear=date.today().year + 1
strStartYear=strEndYear - 26
print(strStartYear)
print(strEndYear)
x = 0

for scopusID in allIDs:  ##iterate through each publication and retrive the json file
    x = x + 1
    query = "https://api.elsevier.com/content/abstract/citations?scopus_id={0}&date={1}-{2}".format(scopusID,strStartYear,strEndYear)
    print ('processing Scopus ID {0} - {1}'.format(scopusID,query))
    response = requests.get(query, headers = headers) #verify false ignores certificate
    fName = 'citation-{0}.json'.format(scopusID)
    data = json.loads(response.text)
    dbutils.fs.put(directory+fName, response.text, True)
    print ('saved {0}'.format(fName))
    print(x)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Parse the json files and store data into datarames to be cleansed

# COMMAND ----------

#Bulk load json files into dataframe and perform the required transformation 
from pyspark.sql.functions import split, explode, explode_outer, col
import re

#location of the json file within the blog storage
directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/citations/"   

#define the schema using the sample json file
df=spark.read.json("/mnt/schema/citation-85092302548.json")
schema = df.schema

#load the entire directory of json file into the data frame
testJsonData = sqlContext.read.schema(schema).json(directory)


yeardf = testJsonData.withColumn("scopus_id",explode("abstract-citations-response.identifier-legend.identifier")).withColumn("year",explode("abstract-citations-response.citeColumnTotalXML.citeCountHeader.columnHeading")).select("Scopus_id.scopus_id","year.$")

countsdf = testJsonData.withColumn("scopus_id",explode("abstract-citations-response.identifier-legend.identifier")).withColumn("counts",explode("abstract-citations-response.citeColumnTotalXML.citeCountHeader.columnTotal")).select("Scopus_id.scopus_id","counts.$")




# COMMAND ----------

# MAGIC %md
# MAGIC Ensure the data in the two datararmes (year and counts) are consistent and can be merged

# COMMAND ----------



# COMMAND ----------

assert(yeardf.count()==countsdf.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Join the two dataframes based on unique sequence number

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

contsdf = countsdf.withColumn("id", monotonically_increasing_id()).withColumnRenamed("Scopus_id","cScopus_id").withColumnRenamed("$","count")

yeardf = yeardf.withColumn("id", monotonically_increasing_id()).withColumnRenamed("$","year")

cntyeardf = contsdf.join(yeardf, on="id")

cntyeardf.createOrReplaceTempView("vwcitbyyear")



# COMMAND ----------

countsdf.select("*").where("scopus_id=='33847632958'").show(100)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM vwcitbyyear

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT scopus_id,count(*)
# MAGIC FROM vwcitbyyear
# MAGIC WHERE count > 0
# MAGIC GROUP BY scopus_id
# MAGIC ORDER BY count(*) desc

# COMMAND ----------

# MAGIC %md
# MAGIC ####Populate the cleansed data into silver table

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --temp table for staging/transoformation 
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_CITATION_COUNT_BY_YEAR(
# MAGIC DOCUMENT_ID string not null,
# MAGIC YEAR_LBL string not null,
# MAGIC CITATION_CNT int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_CITATION_COUNT_BY_YEAR';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE CITSM.s_CITATION_COUNT_BY_YEAR;
# MAGIC 
# MAGIC INSERT INTO CITSM.s_CITATION_COUNT_BY_YEAR
# MAGIC (
# MAGIC SELECT 
# MAGIC   scopus_id as document_id,
# MAGIC   year as year_lbl,
# MAGIC   count as citation_cnt
# MAGIC FROM vwcitbyyear
# MAGIC );
# MAGIC 
# MAGIC OPTIMIZE  CITSM.s_CITATION_COUNT_BY_YEAR;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Validation 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CITATION_CNT FROM CITSM.s_CITATION_COUNT_BY_YEAR WHERE DOCUMENT_ID='33847632958' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct document_id) from CITSM.s_CITATION_COUNT_BY_YEAR

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct document_id) 
# MAGIC from CITSM.s_CITATION_COUNT_BY_YEAR
# MAGIC WHERE citation_cnt <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  CITSM.g_elsevier_CITATIONS_BY_YEAR WHERE CITATION_CNT > 0 ORDER BY CITATION_CNT desc
# MAGIC 
# MAGIC --SELECT * FROM CITSM.g_elsevier_CITATIONS_BY_YEAR WHERE DOCUMENT_ID='73649140927' -- ORDER BY YEAR

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT YEAR_LBL,SUM(CITATION_CNT)
# MAGIC FROM CITSM.g_elsevier_CITATIONS_BY_YEAR 
# MAGIC GROUP BY YEAR_LBL
# MAGIC ORDER BY YEAR_LBL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DOCUMENT_ID,SUM(CITATION_CNT)
# MAGIC FROM CITSM.g_elsevier_CITATIONS_BY_YEAR 
# MAGIC GROUP BY DOCUMENT_ID
# MAGIC ORDER BY SUM(CITATION_CNT) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CITSM.g_elsevier_CITATIONS_BY_YEAR WHERE DOCUMENT_ID='73649140927'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM  CITSM.g_elsevier_publication -- WHERE DOCUMENT_ID NOT IN (SELECT DOCUMENT_ID FROM  CITSM.g_elsevier_publication)
