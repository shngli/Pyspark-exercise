# Pyspark Exercise

Data preprocessing demo with PySpark on Azure Databricks.

## 1. Environment Setup and SparkSession Creation

- Install PySpark: `pip install pyspark`
- Start a SparkSesison: `from pyspark.sql import SparkSession; spark = SparkSession.builder.appName("ETL Process").getOrCreate()`

## 2. Data Extraction

- Read Data from CSV: `df=spark.read.csv("path/to/csv", inferSchema=True, header=True)`
- Read Data from JSON: `df=spark.read.json("path/to/json")`
- Read Data from Parquet: `df=spark.read.parquet("path/to/parquet")`
- Read Data from a Database: `df=spark.read.format("jdbc".option("url", jdbc_url).option("dbtable", "table_name").option("user", "username").option("password", "password").load()`
