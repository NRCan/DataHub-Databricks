# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Elsevier Authors

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
# MAGIC #### Prepare the delta tables 

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC --temp table for staging/transoformation
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_INT_ELSEVIER_AUTHOR_PUBLICATION (
# MAGIC AUTHOR_ID string,
# MAGIC DOCUMENT_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_INT_ELSEVIER_AUTHOR_PUBLICATION';
# MAGIC 
# MAGIC 
# MAGIC --temp table for staging/transoformation 
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_INT_ELSEVIER_AUTHOR (
# MAGIC AUTHOR_ID string,
# MAGIC FIRST_NM string,
# MAGIC LAST_NM string,
# MAGIC FULLNAME string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_INT_ELSEVIER_AUTHOR';
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_ELSEVIER_AUTHOR_PUBLICATION (
# MAGIC AUTHOR_ID string not null,
# MAGIC DOCUMENT_ID string not null
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_AUTHOR_PUBLICATION';
# MAGIC 
# MAGIC  
# MAGIC CREATE TABLE IF NOT EXISTS CITSM.s_ELSEVIER_AUTHOR (
# MAGIC AUTHOR_ID string not null,
# MAGIC FIRST_NM string,
# MAGIC LAST_NM string,
# MAGIC FULLNAME string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/dev/s_ELSEVIER_AUTHOR';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parse the json files and insert the records into the a delta table

# COMMAND ----------

from pyspark.sql.functions import split, explode, monotonically_increasing_id
import re

directory = "/mnt/bronze/dev/elsevier-json/" + datestring + "/publications/"

pattern = ".*scopus-.*\.json"

df=spark.read.json(directory +  "scopus-60017962-0.json")
schema = df.schema

testJsonData = sqlContext.read.schema(schema).json(directory)
newDf = testJsonData.withColumn("search-results", explode("search-results.entry")).select("search-results.*")
authorDf = newDf.withColumn("author", explode("author")) #.select("UT", "Author.*").distinct()
    
authorDf.createOrReplaceTempView("AuthorsView")

spark.sql("""truncate table  CITSM.s_int_ELSEVIER_AUTHOR_PUBLICATION""")

spark.sql("""truncate table  CITSM.s_int_ELSEVIER_AUTHOR""")

##insert into staging table in silver  
spark.sql("""insert into  CITSM.s_int_ELSEVIER_AUTHOR_PUBLICATION
                  select 
                    cast(`author`.`authid` as string) as AUTHOR_ID,
                    cast(substring(`dc:identifier`,11) as string) as DOCUMENT_ID
                    from AuthorsView""")
    
spark.sql("""insert into  CITSM.s_int_ELSEVIER_AUTHOR
                  select 
                     cast(`author`.`authid` as string) as AUTHOR_ID,
                    `author`.`given-name` as FIRST_NM,
                    `author`.`surname` as LAST_NM,
                     `author`.`authname` as FULLNAME
                    from AuthorsView""")





# COMMAND ----------

display(authorDf)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM citsm.g_elsevier_author

# COMMAND ----------

# MAGIC %md
# MAGIC #### Deduplicate Author data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data anomaly check. Discrepancies in authors names with the same authId
# MAGIC SELECT a.*
# MAGIC FROM CITSM.s_int_elsevier_author a
# MAGIC INNER JOIN (
# MAGIC   SELECT author_id,COUNT(*)
# MAGIC   FROM (
# MAGIC       SELECT author_id,fullname,COUNT(*)
# MAGIC       FROM  CITSM.s_int_elsevier_author
# MAGIC       GROUP BY author_id,fullname
# MAGIC       )
# MAGIC   GROUP BY author_id
# MAGIC   HAVING COUNT(*) > 1
# MAGIC ) sub
# MAGIC ON sub.author_id = a.author_id
# MAGIC ORDER BY author_id

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

##append unique sequence id to the the records 
dfTempAuthor = sqlContext.sql("SELECT * FROM  CITSM.s_int_ELSEVIER_AUTHOR")
dfTempAuthor = dfTempAuthor.select("*").withColumn("seqNo", monotonically_increasing_id())

dfTempAuthor.createOrReplaceTempView("AuthorsViewWithSeq")
  

# COMMAND ----------

# MAGIC %sql
# MAGIC --ensure seq number is unique 
# MAGIC SELECT seqNo FROM  AuthorsViewWithSeq
# MAGIC GROUP BY seqNo
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Populate final target table for the Author

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC truncate table  CITSM.s_elsevier_author;
# MAGIC 
# MAGIC --deduplicate authors based on authId and insert into the target table
# MAGIC INSERT INTO  CITSM.s_elsevier_author
# MAGIC SELECT author_id,first_nm,last_nm,fullname
# MAGIC FROM (
# MAGIC       SELECT author_id,first_nm,last_nm,fullname
# MAGIC             ,dense_rank()  OVER (PARTITION BY author_id ORDER BY seqNo ) as rowid
# MAGIC       FROM AuthorsViewWithSeq
# MAGIC       )
# MAGIC WHERE rowid='1'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Validation check for Author

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --ensure only one row per authId
# MAGIC SELECT author_id,COUNT(*)
# MAGIC FROM  CITSM.s_elsevier_author
# MAGIC GROUP BY author_id
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Populate Author-Publication cross reference table

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE  CITSM.s_elsevier_author_publication;
# MAGIC 
# MAGIC --deduplicate rows
# MAGIC INSERT INTO  CITSM.s_elsevier_author_publication 
# MAGIC SELECT DISTINCT author_id,document_id 
# MAGIC FROM  CITSM.s_int_elsevier_author_publication;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(distinct document_id) FROM  CITSM.s_elsevier_author_publication;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check for duplicates in the Author-Publication table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT author_id,document_id
# MAGIC FROM  CITSM.s_elsevier_author_publication 
# MAGIC GROUP BY author_id,document_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --data anomoly check: all the authors in publication-authors (cross ref table) are in the authors table
# MAGIC SELECT *
# MAGIC FROM  CITSM.s_elsevier_author_publication 
# MAGIC   LEFT OUTER JOIN  CITSM.s_elsevier_author 
# MAGIC     ON  CITSM.s_elsevier_author_publication.author_id =  CITSM.s_elsevier_author.author_id
# MAGIC WHERE  CITSM.s_elsevier_author.author_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM  CITSM.s_elsevier_author WHERE author_id=7201623599