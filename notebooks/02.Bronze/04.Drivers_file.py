# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01.Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01.Miscellaneous/Support_functions

# COMMAND ----------

table_name = 'drivers'
source_file = f'{landing_folder_path}{table_name}.json'
table_path = f'{catalog_name}.{bronze_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Crear esquema
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([StructField('givenName', StringType(), True),
                       StructField('familyName', StringType(), True)])

myschema = StructType([StructField('driverId', StringType(), True),
                       StructField('name', name_schema, True),
                       StructField('dateOfBirth', DateType(), True),
                       StructField('nationality', StringType(), True),
                       StructField('url', StringType(), True)])

# COMMAND ----------

# DBTITLE 1,Leer archivo de Landing
drivers_df = read_json_file(source_file,myschema)

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

# DBTITLE 1,Escribir como Delta en Bronze
write_file(drivers_final_df, table_path)

# COMMAND ----------

# DBTITLE 1,Revisar tabla bronze
# display(spark.table(table_path))
