# Analyzing BART Daily Ridership: Insights from BigQuery SQL Queries

# Setup Environment: 

1. Set up a Python environment with necessary libraries such as pandas, 
2. Setup Aiflow 
3. Use the DAG files to run the worflows
4. Use Terraform variables to setup Google Cloud
5. Use attached Tableau Workbook to generate reports using the linked Google Bigquery Datawarehouse{Data Source} 

# Used Technologies 

For this project I decided to use the following tools:

Data Orchestration: DAGs using Airlfow;
Terraform - as Infrastructure-as-Code (IaC) tool;
Google Compute Engine - as a virtual machine;
Google Cloud Storage (GCS) - for storage as Data Lake;
Google BigQuery - for Data Warehousing;
dbt - for the transformation of raw data in refined data;
Tableau - for visualizations.

# Problem Statement
I developed a data pipeline to retrieve daily BART ridership data from their website
process it using Python scripts within Airflow, and load it into Google Cloud Storage.
Subsequently, the transformed data is ingested into BigQuery tables for further analysis. 
With the help of dbt, the raw data undergoes transformation, 
and the insights derived are visualized using Tableau.


Tableau Dashboard:
https://github.com/pilanpra/DE-Projects/blob/eb0eed86da9ba4e7710b7c7408bda1c83a63cc1f/Bootcamp2024Project/Reporting%20Files/Dashboard%202023.png
