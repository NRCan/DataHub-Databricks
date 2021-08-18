# DataHub-Databricks

[NRCan Datahub](https://github.com/NRCan/datahub-portal) leverages [Databricks](https://databricks.com/) for Data Projects to enable data ingestion, wrangling, processing, and basic visualizations. 

This repository contains Sample Projects, Sample Code and demos.

## Sample Projects
- **CITSM** - Databricks is used to ingest data from Elsevier API of publications related to NRCAN and related citation data and structures the result into Hive tables. Power BI is used to connect to the data source and summarize the results.
- **Departmental Resource Framework** - Ingest project tracker data from an excel file and load into a hive table (delta table) to be consumbed by Power BI for analysis

## sample code
- Load GeoChem Data.ipynb Contails sample code for loading extracting data from multiple worksheets in Excel spreadsheet using python (pandas)

## Demo
- Contains code snippets used by Datahub to demo Databricks and its capabilities
