# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01_Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01_Miscellaneous/Support_functions

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# DBTITLE 1,Dinamico
table_name = 'constructors'
join_key = 'constructor_id'
source_table = f'{catalog_name}.{bronze_schema}.{table_name}'
table_path = f'{catalog_name}.{silver_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Leer tabla delta como dataframe
df = spark.read.table(source_table).filter(F.col('batch_id')==v_batch_id)

# COMMAND ----------

# DBTITLE 1,Limpiar dataframe
from pyspark.sql import functions as F

df_constructors = (df.dropDuplicates()
                    .withColumn("nationality", F.initcap("nationality"))
                    .select(
                            F.col('constructorId').alias('constructor_id'),
                            F.col('name').alias('constructor_name'),
                            F.col('nationality'),
                            F.col('Ingestion_timestamp').alias('ingestion_timestamp'),
                            F.col('Source_file').alias('source_file'),
                            F.col('batch_id')))

# COMMAND ----------

# DBTITLE 1,Escribir df en silver
write_to_silver(df_constructors, table_path, join_key)

# COMMAND ----------

# display(spark.table(table_path))
