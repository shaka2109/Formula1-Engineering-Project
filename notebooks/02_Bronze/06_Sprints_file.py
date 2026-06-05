# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01_Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01_Miscellaneous/Support_functions

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

table_name = 'sprints'
source_file = f'{landing_folder_path}{v_batch_id}/{table_name}/'
table_path = f'{catalog_name}.{bronze_schema}.{table_name}'


# COMMAND ----------

# DBTITLE 1,Crear esquema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

myschema = StructType([StructField('date', DateType(), True),
                       StructField('raceName', StringType(), True),
                       StructField('round', IntegerType(), True),
                       StructField('season', IntegerType(), True),
                       StructField('url', StringType(), True),
                       StructField('constructorId', StringType(), True),
                       StructField('driverId', StringType(), True),
                       StructField('grid', IntegerType(), True),
                       StructField('laps', IntegerType(), True),
                       StructField('number', IntegerType(), True),
                       StructField('points', DoubleType(), True),
                       StructField('position', IntegerType(), True),
                       StructField('positionText', StringType(), True),
                       StructField('status', StringType(), True)])

# COMMAND ----------

# DBTITLE 1,Leer archivo de Landing
sprints_df = read_json_multiline_file(source_file,myschema)

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

# DBTITLE 1,Escribir como Delta en Bronze
write_delta(sprints_final_df, table_path, v_batch_id)

# COMMAND ----------

# DBTITLE 1,Revisar tabla bronze
# display(spark.table(table_path))
