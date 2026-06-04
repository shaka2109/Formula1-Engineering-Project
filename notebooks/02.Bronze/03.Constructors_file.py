# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01.Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01.Miscellaneous/Support_functions

# COMMAND ----------

table_name = 'constructors'
source_file = f'{landing_folder_path}{table_name}.json'
table_path = f'{catalog_name}.{bronze_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Crear esquema
myschema = 'constructorId STRING, name STRING, nationality STRING, url STRING' 

# COMMAND ----------

# DBTITLE 1,Leer archivo de Landing
constructores_df = read_json_file(source_file,myschema)

# COMMAND ----------

constructores_final_df = add_ingestion_metadata(constructores_df)

# COMMAND ----------

# DBTITLE 1,Escribir como Delta en Bronze
write_file(constructores_final_df, table_path)

# COMMAND ----------

# DBTITLE 1,Revisar tabla bronze
# display(spark.table(table_path))
