# Pyspark Exercise

Data preprocessing demo with PySpark on Azure Databricks. **TBD: pyspark project notebooks**

## ETL Processes with PySpark

### 1. Environment Setup and SparkSession Creation

- Install PySpark: `pip install pyspark`
- Start a SparkSesison: `from pyspark.sql import SparkSession; spark = SparkSession.builder.appName("ETL Process").getOrCreate()`

### 2. Data Extraction

- Read Data from CSV: `df = spark.read.csv("path/to/csv", inferSchema=True, header=True)`
- Read Data from JSON: `df = spark.read.json("path/to/json")`
- Read Data from Parquet: `df = spark.read.parquet("path/to/parquet")`
- Read Data from a Database: `df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable","table_name").option("user","username").option("password","password").load()`
- Reading from Multiple Sources: `df = spark.read.format("format").option("option","value").load(["path1","path2"])`

### 3. Data Transformation

- Selecting Columns: `df.select("column1","column2")`
- Filtering Data: `df.filter(df["column"] > value)`
- Adding New Columns: `df.withColumn("new_column", df["column"]+10)`
- Renaming Columns: `df.withColumnRenamed("old_name","new_name")`
- Grouping and Aggregating Data: `df.groupBy("column").agg({"column2":"sum"})`
- Joining DataFrames: `df1.join(df2, df1["id"]==df2["id"])`
- Sorting Data: `df.orderBy(df["column"].desc())`
- Removing Duplicates: `df.dropDuplicates()`

### 4. Handling Missing Values

- Dropping Rows with Missing Values: `df.na.drop()`
- Filling Missing Values: `df.na.fill(value)`
- Replacing Values: `df.na.replace(["old_value"],["new_value"])`

### 5. Data Type Conversion

- Changing Column Types: `df.withColumn("column", df["column"].cast("new_type"))`
- Parsing Dates: `from pyspark.sql.functions import to_date; df.withColumn("date", to_date(df["date_string"]))`

### 6. Working with Dates and Times

- Date Arithmetic: `df.withColumn("new_date", F.col("date_col") + F.expr("interval 1 day"))`
- Date Truncation and Formatting: `df.withColumn("month", F.trunc("month", "date_col"))`

### 7. Data Manipulations

- Using SQL Queries: `df.createOrReplaceTempView("table"); spark.sql("SELECT * FROM table WHERE column > value")`
- Window Functions: `from pyspark.sql.window import Window; from
pyspark.sql.functions import row_number; df.withColumn("row",
row_number().over(Window.partitionBy("column").orderBy("other_colum
n")))`
- Pivot Tables: `df.groupBy("column").pivot("pivot_column").agg({"column2":"sum"})`
- Using Temporary Views for SQL Queries: `df.createOrReplaceTempView("temp_view"); spark.sql("SELECT * FROM temp_view WHERE col > value")`

### 8. Data Writing

- Writing to CSV: `df.write.csv("path/to/output")`
- Writing to JSON: `df.write.json("path/to/output")`
- Writing to Parquet: `df.write.parquet("path/to/output")`
- Writing to a Database: `df.write.format("jdbc").option("url", jdbc_url).option("dbtable","table_name").option("user","username").option("password","password").save()`

### 9. Data Partitioning and Bucketing

- Partitioning Data for Efficient Storage: `df.write.partitionBy("date_col").parquet("path/to/output")`
- Bucketing Data for Optimized Query Performance: `df.write.bucketBy(42,"key_col").sortBy("sort_col").saveAsTable("bucketed_table")`

### 10. Data Filtering

- Filtering with Multiple Conditions: `df.filter((df["col1"] > value) & (df["col2"] < other_value))`
- Using Column Expressions: `from pyspark.sql import functions as F; df.filter(F.col("col1").like("%pattern%"))`

### 11. Data Aggregation and Summarization

- Aggregations: `df.groupBy("group_col").agg({"num_col1":"sum", "num_col2":"avg"})`
- Rollup and Cube for Multi-Dimensional Aggregation: `df.rollup("col1","col2").sum(), df.cube("col1","col2").mean()`

### 12. Advanced Window Functions

- Window Functions for Running Totals and Moving Averages: `from pyspark.sql.window import Window; windowSpec = Window.partitionBy("group_col").orderBy("date_col"); df.withColumn("cumulative_sum", F.sum("num_col").over(windowSpec))`
- Ranking and Row Numbering: `df.withColumn("rank", F.rank().over(windowSpec))`

### 13. Working with Complex Data Types

- Exploding Arrays: `from pyspark.sql.functions import explode; df.select(explode(df["array_column"]))`
- Handling Struct Fields: `df.select("struct_column.field1","struct_column.field2")`
- Nested JSON Parsing: `from pyspark.sql.functions import json_tuple; df.select(json_tuple("json_column","field1","field2"))`

### 14. Handling Nested and Complex Structures

- Working with Arrays and Maps: `df.select(F.explode("array_col")), df.select(F.col("map_col")["key"])`
- Flattening Nested Structures: `df.selectExpr("struct_col.*")`

### 15. Custom Transformations with UDFs
- Defining a UDF: `from pyspark.sql.functions import udf; @udf("return_type") def my_udf(column): return transformation`
- Applying UDF on DataFrame: `df.withColumn("new_column", my_udf(df["column"]))`

### 16. Text Processing

- Regular Expressions for Text Data: `df.withColumn("extracted", F.regexp_extract("text_col", "(pattern)", 1))`
- Tokenizing Text Data: `from pyspark.ml.feature import Tokenizer; Tokenizer(inputCol="text_column", outputCol="words").transform(df)`
- TF-IDF on Text Data: `from pyspark.ml.feature import HashingTF, IDF; HashingTF(inputCol="words", outputCol="rawFeatures").transform(df)`

### 17. Stream Processing
- Reading from a Stream: `dfStream = spark.readStream.format("source").load()`
- Writing to a Stream: `dfStream.writeStream.format("console").start()`

### 18. Performance Tuning

- Caching Data: `df.cache()`
- Broadcasting a DataFrame for Join Optimization: `from pyspark.sql.functions import broadcast; df1.join(broadcast(df2), df1["id"] == df2["id"])`
- Repartitioning Data: `df.repartition(10)`
- Coalescing Partitions: `df.coalesce(1)`

### 19. Debugging and Error Handling

- Showing Execution Plan: `df.explain()`
- Catching Exceptions during Read: Implement try-except blocks during data reading operations.
