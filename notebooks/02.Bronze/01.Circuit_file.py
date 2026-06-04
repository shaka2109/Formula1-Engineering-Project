# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01.Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01.Miscellaneous/Support_functions

# COMMAND ----------

table_name = 'circuits'
source_file = f'{landing_folder_path}{table_name}.csv'
table_path = f'{catalog_name}.{bronze_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Crear esquema
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

myschema = StructType([StructField('circuitId', StringType(), True),
                       StructField('url', StringType(), True),
                       StructField('circuitName', StringType(), True),
                       StructField('lat', DoubleType(), True),
                       StructField('long', DoubleType(), True),
                       StructField('locality', StringType(), True),
                       StructField('country', StringType(), True)])

# COMMAND ----------

# DBTITLE 1,Leer archivo de Landing
circuits_df = read_csv_file(source_file,myschema)

# COMMAND ----------

# DBTITLE 1,Agregar columnas de auditar
circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

# DBTITLE 1,Escribir como Delta en Bronze
write_file(circuits_final_df, table_path)

# COMMAND ----------

# DBTITLE 1,Revisar tabla bronze
# display(spark.table(table_path))
