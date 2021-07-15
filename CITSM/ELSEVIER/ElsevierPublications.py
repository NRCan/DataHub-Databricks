# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Elsevier Publication

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setup the parameters for manual run of the notebook

# COMMAND ----------

datestring = '2021-07-08'
runtype = 'prod'  ##test/prod

# COMMAND ----------

# MAGIC %md
# MAGIC ####Set the run date based on the parameter received from ADF. This cell is not to be run when executing this notbeook manually

# COMMAND ----------

datestring = dbutils.widgets.get("datestring")   ##run date gets overwritten based on the parameter passed from ADF
runtype = dbutils.widgets.get("runtype")   ##run mode gets overwritten 
print(datestring)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Check if Affiliation Data is available 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CITSM.s_Elsevier_Affiliation --where document_count > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest json files and store them in the blob storage

# COMMAND ----------

print(datestring)

# COMMAND ----------

dbutils.fs.rm("/mnt/bronze/dev/elsevier-json/" + datestring + "/publications/",True)

# COMMAND ----------

import requests
import json
import io
import glob
from collections import namedtuple

directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/publications/"

if runtype == 'prod':
  allIDs = [int(i.AFFILIATION_ID) for i in spark.sql("select AFFILIATION_ID from CITSM.s_Elsevier_Affiliation").collect()]
else:
  allIDs = ["60017962"]
  
# Get all institutions affiliated with 'Natural Resources Canada'
headers = {'X-ELS-APIKey':dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierAPIKey"),'X-ELS-Insttoken': dbutils.secrets.get(scope = "datalake-key-dev", key = "elsevierInsttoken")}

# 2. get scopus for documents using affiliate list
# this query seems to return a max of 25 results at a time, how sad
# it maybe better to put all the affIds into the query using the OR clause

documentDict = {}
authorDict = {}
documentToAuthorDict = {}
result = []

totalApiCalls = 0
for affId in allIDs:
    query = "https://api.elsevier.com/content/search/scopus?query=AF-ID({0})&view=complete&start=0".format(affId)
    print ('processing AF-ID {0} - {1}'.format(affId,query))
    response = requests.get(query, headers = headers) #verify false ignores certificate
    data = json.loads(response.text)
    totalResults = data['search-results']['opensearch:totalResults']
   
    
    #if runmode is test then extract only the specified subset of the files
    if runtype != 'prod':
      totalApiCalls = 5
    else:
       totalApiCalls = int((int(totalResults)/25)) + 1

    
    for x in range(totalApiCalls):
        start = x * 25
        query = "https://api.elsevier.com/content/search/scopus?query=AF-ID({0})&view=complete&start={1}".format(affId,start)
        response = requests.get(query, headers = headers) #verify false ignores certificate
        fName = 'scopus-{0}-{1}.json'.format(affId,x)
        dbutils.fs.put(directory+fName, response.text, True)    
        print ('Saved {0}'.format(directory+fName))
        
    print ('AF-ID {0} completed'.format(affId))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare the delta tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_ELSEVIER_PUBLICATION(
# MAGIC DOCUMENT_ID string not null,
# MAGIC DOCUMENT_DESC string,
# MAGIC DOCUMENT_TITLE_LBL string,
# MAGIC ELECTRONIC_ID string,
# MAGIC ARTICLE_NBR string,
# MAGIC SUB_TYPE_ID string,
# MAGIC SUB_TYPE_LBL string,
# MAGIC PRISM_ISSN_LBL string,
# MAGIC PRISM_COVER_DT DATE,
# MAGIC FA_LBL string,
# MAGIC PRISM_DIGITAL_OBJECT_ID string,
# MAGIC PRISM_COVER_DISPLAY_DT DATE,
# MAGIC AUTHOR_KEYWORDS_DESC string,
# MAGIC DOCUMENT_CREATOR_NM string,
# MAGIC PRISM_PAGE_RANGE_LBL string,
# MAGIC PRISM_URL string,
# MAGIC SOURCE_ID string,
# MAGIC PRISM_PUBLICATION_NM string,
# MAGIC CITED_BY_CNT string,
# MAGIC PUBLIC_ITEM_ID string,
# MAGIC OPEN_ACCESS_IND INT,
# MAGIC OPEN_ACCESS_IND_LBL string,
# MAGIC FUND_NBR string,
# MAGIC PRISM_VOLUME_NBR string,
# MAGIC PRIMS_AGGREGATION_TYPE_LBL string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_PUBLICATION';
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION(
# MAGIC DOCUMENT_ID string not null,
# MAGIC AFFILIATION_ID string not null
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_AFFILIATION_PUBLICATION';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parse the json files and insert the records into the a delta table 

# COMMAND ----------

from  pyspark.sql.functions import input_file_name
schema = "value STRING"
raw_publication_df = spark.read.format("text").schema(schema).load("/mnt/bronze/dev/elsevier-json/" + datestring + "/publications/" ).withColumn("filename", input_file_name())
raw_publication_df.count()

# COMMAND ----------

from pyspark.sql.functions import from_json,col

publication_schema_df = spark.read.json("/mnt/schema/scopus-60017962-0.json")
schema = publication_schema_df.schema

b_json_publication_df = raw_publication_df.withColumn("nested_json",from_json(col("value"),schema))


# COMMAND ----------

import re
from pyspark.sql import functions as F 
from pyspark.sql.functions import split, explode,lit

int_publication_df = b_json_publication_df.withColumn("AFID_ORIG",col("nested_json.search-results.opensearch:Query.@searchTerms"))

affililiation_publication_df = int_publication_df.withColumn("search-results", explode("nested_json.search-results.entry")).withColumn("AFFILIATION_ID",F.regexp_extract(int_publication_df.AFID_ORIG, '\(([^)]+)', 1)).select("AFFILIATION_ID","filename","search-results.*").withColumn("identifier",col("dc:identifier")).withColumn("DOCUMENT_ID",F.regexp_extract(col("identifier"), ':(.*)', 1))

affililiation_publication_df.createOrReplaceTempView("VW_AFFILIATION_PUBLICATION")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM VW_AFFILIATION_PUBLICATION where DOCUMENT_ID='84903491365'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Publications - Deduplicate 

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

##append unique sequence id to the the records 
dfTempPublication = sqlContext.sql("SELECT * FROM VW_AFFILIATION_PUBLICATION")
dfTempPublication = dfTempPublication.select("*").withColumn("seqNo", monotonically_increasing_id())

dfTempPublication.createOrReplaceTempView("PublicationsViewWithSeq")

# COMMAND ----------

# MAGIC %sql
# MAGIC --ensure seq number is unique 
# MAGIC SELECT seqNo FROM  PublicationsViewWithSeq
# MAGIC GROUP BY seqNo
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ####Publications - Populate the final target 

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table CITSM.s_elsevier_publication;
# MAGIC 
# MAGIC 
# MAGIC --deduplicate publication based on document_id and insert into the target table
# MAGIC INSERT INTO CITSM.s_elsevier_publication
# MAGIC SELECT        
# MAGIC        DOCUMENT_ID,
# MAGIC       `dc:description` as DOCUMENT_DESC,
# MAGIC       `dc:title` as DOCUMENT_TITLE_LBL,
# MAGIC       `eid` as ELECTRONIC_ID,
# MAGIC       `article-number` as ARTICLE_NBR,
# MAGIC       `subtype` as SUB_TYPE_ID,
# MAGIC       `subtypeDescription` as SUB_TYPE_LBL,
# MAGIC       `prism:issn` PRISM_ISSN_LBL,
# MAGIC       cast(`prism:coverDate` as date) as PRISM_COVER_DT,
# MAGIC       `@_fa` FA_LBL,
# MAGIC       `prism:doi` as PRISM_DIGITAL_OBJECT_ID,
# MAGIC       cast(`prism:coverDisplayDate` as date) as PRISM_COVER_DISPLAY_DT,
# MAGIC       `authkeywords` as AUTHOR_KEYWORDS_DESC,
# MAGIC       `dc:creator` DOCUMENT_CREATOR_NM,
# MAGIC       `prism:pageRange` PRISM_PAGE_RANGE_LBL,
# MAGIC       `prism:url` as PRISM_URL,
# MAGIC       `source-id` as SOURCE_ID,
# MAGIC       `prism:publicationName` as PRISM_PUBLICATION_NM,
# MAGIC       `citedby-count` as CITED_BY_CNT,
# MAGIC       `pii` as PUBLIC_ITEM_ID,
# MAGIC       `openaccessFlag` as OPEN_ACCESS_IND,
# MAGIC       case when openaccessFlag then 'Open Access' else 'Not Open Access' end as OPEN_ACCESS_IND_LBL,
# MAGIC       `fund-no` as FUND_NBR,
# MAGIC       `prism:volume` as PRISM_VOLUME_NBR,
# MAGIC       `dc:title` as DOCUMENT_TITLE_LBL
# MAGIC FROM (
# MAGIC       SELECT PublicationsViewWithSeq.*
# MAGIC             ,dense_rank()  OVER (PARTITION BY DOCUMENT_ID ORDER BY seqNo ) as rowid
# MAGIC       FROM PublicationsViewWithSeq
# MAGIC       )
# MAGIC WHERE rowid='1'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Publication - Data Validation Check 

# COMMAND ----------

# MAGIC %sql
# MAGIC --ensure seq number is unique 
# MAGIC SELECT document_id FROM CITSM.s_elsevier_publication
# MAGIC GROUP BY document_id
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM CITSM.s_elsevier_publication

# COMMAND ----------

# MAGIC %md
# MAGIC #### Publication x Affiliation - Deduplicate and populate final target table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION;
# MAGIC TRUNCATE TABLE CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION;
# MAGIC 
# MAGIC INSERT INTO CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION (
# MAGIC   SELECT  distinct document_id,affiliation_id
# MAGIC   FROM PublicationsViewWithSeq
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Publication x Affiliation - Data Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION
# MAGIC GROUP BY DOCUMENT_ID,AFFILIATION_ID
# MAGIC HAVING COUNT(*) > 1
