# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01_Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01_Miscellaneous/Support_functions

# COMMAND ----------

# DBTITLE 1,Dinamico
table_name = 'races'
source_file = f'{landing_folder_path}{table_name}.csv'
table_path = f'{catalog_name}.{bronze_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Crear esquema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

myschema = StructType([StructField('season', IntegerType(), True),
                       StructField('round', IntegerType(), True),
                       StructField('url', StringType(), True),
                       StructField('raceName', StringType(), True),
                       StructField('date', DateType(), True),
                       StructField('circuitId', StringType(), True)])

# COMMAND ----------

# DBTITLE 1,Leer archivo de Landing
races_df = read_csv_file(source_file,myschema)

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

# DBTITLE 1,Escribir como Delta en Bronze
write_file(races_final_df, table_path)

# COMMAND ----------

# DBTITLE 1,Revisar tabla bronze
# display(spark.table(table_path))
