# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 1) Get all institutions affiliated with "Natural Resources Canada"
# MAGIC 
# MAGIC Get a list of all the affiliation Ids
# MAGIC https://api.elsevier.com/content/search/affiliation?query=affil(Natural%20Resources%20Canada)&apiKey=d3a13849b76deb94c3d0d0faf63c4da4
# MAGIC 
# MAGIC 2) Generate a Get query to scopus to get all documents for all of the affiliates
# MAGIC 
# MAGIC http://api.elsevier.com/content/search/scopus?query=AF-ID ( ""Natural Resources Canada"" 60023084 )....
# MAGIC you can use an OR in the query, or just loop through each affiliate Id and make multiple calls
# MAGIC 3) Using the Get query, get the total number of documents.
# MAGIC 
# MAGIC 4) Using the same get query, information for each document can be obtained.
# MAGIC 
# MAGIC 5) For each of the publication, retrieve Authors associated with each of the publications
# MAGIC 
# MAGIC 6) For each of the publication, retrieve all the Citations and Social Mentions associated with the publications
# MAGIC 
# MAGIC 7) Using the eid, create a get query to obtain citations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Elsevier Affiliation

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
# MAGIC #### Ingest json files and store them on the blob storage

# COMMAND ----------

import requests
import json

headers = {'X-ELS-APIKey':dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierAPIKey"),'X-ELS-Insttoken': dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierInsttoken")}
response = requests.get("https://api.elsevier.com/content/search/affiliation?query=affil(Natural%20Resources%20Canada)&count=100", headers = headers)

directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/affliations/"

data = json.loads(response.text)

totalResults = data['search-results']['opensearch:totalResults']
totalApiCalls = int((int(totalResults)/100)) + 1

AffIdList = []

for x in range(totalApiCalls):
    start = x * 100
    query = "https://api.elsevier.com/content/search/affiliation?query=affil(Natural%20Resources%20Canada)&count=100&start="+str(start)
    print (query)
    response = requests.get(query, headers = headers) #verify false ignores certificate
    fName = 'affiliation-{0}.json'.format(x)
    
    dbutils.fs.put(directory+fName, response.text, True)    

    print ('saved {0}'.format(fName))
    data = json.loads(response.text)
    for entry in data['search-results']['entry']:
        affId = entry["dc:identifier"][15:] 
        if (affId.startswith('600')):
            AffIdList.append(affId)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare the delta tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_ELSEVIER_AFFILIATION(
# MAGIC AFFILIATION_ID int not null,
# MAGIC ELECTRONIC_ID string not null,
# MAGIC PRISM_URL string,
# MAGIC AFFILIATION_NM string,
# MAGIC DOCUMENT_CNT int)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_AFFILIATION';

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE CITSM.s_ELSEVIER_AFFILIATION; 
# MAGIC OPTIMIZE CITSM.s_ELSEVIER_AFFILIATION;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parse the json files and insert the records into the delta table

# COMMAND ----------

from pyspark.sql.functions import split, explode

directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/affliations/"

df=spark.read.json("/mnt/schema/affiliation-0.json")
schema = df.schema

#print(schema)


for x in range(totalApiCalls):    
    record = (100 * x) + 1    
    file = directory + 'affiliation-{0}.json'.format(x)
    testJsonData = sqlContext.read.schema(schema).json(file)
    newDf = testJsonData.withColumn("search-results", explode("search-results.entry")).select("search-results.*")
    newDf.createOrReplaceTempView("AffView")
    

    spark.sql("""insert into CITSM.S_ELSEVIER_AFFILIATION
                 select cast(substr(`dc:identifier`,16) as int) as AFFILIATION_ID,
                   eid as ELECTRONIC_ID,
                   `prism:url` as PRISM_URL,
                   `affiliation-name` as AFFLIATION_NM,
                   `document-count` as DOCUMENT_CNT
                   from AffView""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Validation Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) from CITSM.s_ELSEVIER_AFFILIATION

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT AFFILIATION_ID AS SCOPUS_AFFILIATION_ID, AFFILIATION_NM FROM CITSM.s_ELSEVIER_AFFILIATION ORDER BY AFFILIATION_NM ASC

# COMMAND ----------

# MAGIC %sql