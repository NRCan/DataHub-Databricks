# Databricks notebook source
# MAGIC %md
# MAGIC ###Data Integrity Check For Affliation Data

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.amazon.deequ.VerificationSuite
# MAGIC import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
# MAGIC import com.amazon.deequ.constraints.ConstraintStatus
# MAGIC import org.apache.spark.sql.functions.{col,concat}
# MAGIC 
# MAGIC 
# MAGIC val dfpub = sqlContext.sql("SELECT * FROM CITSM.s_ELSEVIER_AFFILIATION")
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("AFFILIATION_ID") // should never be NULL
# MAGIC       .isComplete("ELECTRONIC_ID") // should never be NULL
# MAGIC       .isUnique("AFFILIATION_ID") // should not contain duplicates
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Integrity Check For Publication Data

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfpub = sqlContext.sql("SELECT * FROM CITSM.s_ELSEVIER_PUBLICATION")
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("DOCUMENT_ID") // should never be NULL
# MAGIC       .isUnique("DOCUMENT_ID") // should not contain duplicates
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Integrity Check For Affiliation x Publication 

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfpub = sqlContext.sql("SELECT a.DOCUMENT_ID as apDocId, c.DOCUMENT_ID as pDocID, a.AFFILIATION_ID apAffID, b.AFFILIATION_ID as afID  FROM CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION a LEFT OUTER JOIN CITSM.s_ELSEVIER_AFFILIATION b ON a.AFFILIATION_ID = b.AFFILIATION_ID LEFT OUTER JOIN CITSM.s_ELSEVIER_PUBLICATION c ON a.DOCUMENT_ID = c.DOCUMENT_ID")
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("apDocId") // should never be NULL
# MAGIC       .isComplete("pDocID") // should never be NULL  -- validating forign key constraint
# MAGIC       .isComplete("apAffID") // should never be NULL 
# MAGIC       .isComplete("afID") // should never be NULL   -- validating forign key constraint 
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Integrity Check For Author

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfpub = sqlContext.sql("SELECT * FROM CITSM.s_ELSEVIER_AUTHOR")
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("AUTHOR_ID") // should never be NULL
# MAGIC       .isUnique("AUTHOR_ID") // should not contain duplicates
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Integrity Check For Author x Publication

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfpub = sqlContext.sql("SELECT a.DOCUMENT_ID as PdocID, b.DOCUMENT_ID as FdocID, a.AUTHOR_ID pAuthID, c.AUTHOR_ID fAuthID FROM CITSM.s_ELSEVIER_AUTHOR_PUBLICATION a LEFT OUTER JOIN CITSM.s_ELSEVIER_PUBLICATION b ON a.DOCUMENT_ID = b.DOCUMENT_ID LEFT OUTER JOIN CITSM.s_ELSEVIER_AUTHOR c ON a.AUTHOR_ID = c.AUTHOR_ID")
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("PdocID") // should never be NULL
# MAGIC       .isComplete("FdocID") // should never be NULL  -- validating forign key constraint
# MAGIC       .isComplete("pAuthID") // should never be NULL 
# MAGIC       .isComplete("fAuthID") // should never be NULL   -- validating forign key constraint 
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Integrity Check For Citation by Year

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfpub = sqlContext.sql("SELECT a.DOCUMENT_ID pDocId, b.DOCUMENT_ID fDocId,concat(a.DOCUMENT_ID,year_lbl) ukey FROM CITSM.s_CITATION_COUNT_BY_YEAR a LEFT OUTER JOIN CITSM.s_ELSEVIER_PUBLICATION b ON a.DOCUMENT_ID = b.DOCUMENT_ID ") 
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("pDocId") // should never be NULL
# MAGIC       .isComplete("fDocId") // should never be NULL   --forign key
# MAGIC       .isUnique("ukey") // should not contain duplicates  
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Integrity Check For Plumx Metric 

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfpub = sqlContext.sql("SELECT a.DOCUMENT_ID pDocId, b.DOCUMENT_ID fDocId,concat(a.DOCUMENT_ID, '-' ,SUB_CATEGORY_LBL) ukey FROM CITSM.s_ELSEVIER_PLUMX_METRIC a LEFT OUTER JOIN CITSM.s_ELSEVIER_PUBLICATION b ON a.DOCUMENT_ID = b.DOCUMENT_ID ") 
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("pDocId") // should never be NULL
# MAGIC       .isComplete("fDocId") // should never be NULL   --forign key
# MAGIC       .isUnique("ukey") // should not contain duplicates  
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Integrity Check For PlumX Citation Source

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfpub = sqlContext.sql("SELECT a.DOCUMENT_ID pDocId, b.DOCUMENT_ID fDocId,concat(a.DOCUMENT_ID, '-' ,CITATION_SOURCE_LBL ) ukey FROM CITSM.s_ELSEVIER_PLUMX_CITATION_SOURCE a LEFT OUTER JOIN CITSM.s_ELSEVIER_PUBLICATION b ON a.DOCUMENT_ID = b.DOCUMENT_ID ") 
# MAGIC 
# MAGIC val verificationResult = VerificationSuite()
# MAGIC   .onData(dfpub)
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "unit testing the data for intergrity contstraints")
# MAGIC       .hasSize(_ > 0) // expect at-least 1 row
# MAGIC       .isComplete("pDocId") // should never be NULL
# MAGIC       .isComplete("fDocId") // should never be NULL   --forign key
# MAGIC       .isUnique("ukey") // should not contain duplicates  
# MAGIC   )
# MAGIC     .run()
# MAGIC 
# MAGIC if (verificationResult.status == CheckStatus.Success) {
# MAGIC   println("Data validation passed!")
# MAGIC } else {
# MAGIC   println("Errors found in data validation")
# MAGIC 
# MAGIC   val resultsForAllConstraints = verificationResult.checkResults
# MAGIC     .flatMap { case (_, checkResult) => checkResult.constraintResults }
# MAGIC 
# MAGIC   resultsForAllConstraints
# MAGIC     .filter { _.status != ConstraintStatus.Success }
# MAGIC     .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ###Publish Data to the Gold Tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC  select  count(*) from CITSM.s_ELSEVIER_PUBLICATION  t;

# COMMAND ----------

# MAGIC %sql
# MAGIC  select  count(*) from CITSM.g_ELSEVIER_PUBLICATION  t;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from CITSM.s_ELSEVIER_PUBLICATION 

# COMMAND ----------

sqlContext.sql("truncate table CITSM.g_ELSEVIER_AFFILIATION")
sqlContext.sql("truncate table CITSM.g_ELSEVIER_PUBLICATION")
sqlContext.sql("truncate table CITSM.g_ELSEVIER_AFFILIATION_PUBLICATION")
sqlContext.sql("truncate table CITSM.g_ELSEVIER_AUTHOR")
sqlContext.sql("truncate table CITSM.g_ELSEVIER_AUTHOR_PUBLICATION")
sqlContext.sql("truncate table CITSM.g_ELSEVIER_CITATIONS_BY_YEAR")
sqlContext.sql("truncate table CITSM.g_ELSEVIER_PLUMX_METRIC")
sqlContext.sql("truncate table CITSM.g_ELSEVIER_PLUMX_CITATION_SOURCE")


sqlContext.sql("insert into CITSM.g_ELSEVIER_AFFILIATION select t.* from CITSM.s_ELSEVIER_AFFILIATION t")
sqlContext.sql("insert into CITSM.g_ELSEVIER_PUBLICATION select t.* from CITSM.s_ELSEVIER_PUBLICATION t")
sqlContext.sql("insert into CITSM.g_ELSEVIER_AFFILIATION_PUBLICATION select t.* from CITSM.s_ELSEVIER_AFFILIATION_PUBLICATION t")
sqlContext.sql("insert into CITSM.g_ELSEVIER_AUTHOR select t.* from CITSM.s_ELSEVIER_AUTHOR t")
sqlContext.sql("insert into CITSM.g_ELSEVIER_AUTHOR_PUBLICATION select t.* from CITSM.s_ELSEVIER_AUTHOR_PUBLICATION t")
sqlContext.sql("insert into CITSM.g_ELSEVIER_CITATIONS_BY_YEAR select t.* from CITSM.s_CITATION_COUNT_BY_YEAR t")
sqlContext.sql("insert into CITSM.g_ELSEVIER_PLUMX_METRIC select t.* from CITSM.s_ELSEVIER_PLUMX_METRIC t")
sqlContext.sql("insert into CITSM.g_ELSEVIER_PLUMX_CITATION_SOURCE select t.* from CITSM.s_ELSEVIER_PLUMX_CITATION_SOURCE t")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM CITSM.g_ELSEVIER_PUBLICATION