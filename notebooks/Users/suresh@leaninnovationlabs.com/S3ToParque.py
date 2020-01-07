# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC This shows an example of how to load data from S3 location and create a Spark Table for data processing

# COMMAND ----------

# File location and type
file_location = "s3://buzinsights/ebidata_dev_daily_transactions/"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "daily_revenue"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select count(*) from `daily_revenue`

# COMMAND ----------

# Since this table is registered as a temp view, it will only be available to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "daily_revenue"

df.write.format("parquet").saveAsTable("daily_revenue")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select tenant_id, sum(grand_total) from daily_revenue
# MAGIC group by tenant_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select report_date, sum(transaction_total_today) total_amount from daily_revenue
# MAGIC group by report_date;

# COMMAND ----------

