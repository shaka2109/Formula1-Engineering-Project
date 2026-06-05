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
table_name = 'circuits'
source_table = f'{catalog_name}.{bronze_schema}.{table_name}'
table_path = f'{catalog_name}.{silver_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Leer tabla delta como dataframe
df = spark.read.table(source_table)

# COMMAND ----------

# DBTITLE 1,Limpiar dataframe
from pyspark.sql import functions as F

df_circuits = (df.filter(df.circuitId.isNotNull())
                    .dropDuplicates()
                    .withColumn("circuitName", F.initcap("circuitName"))
                    .withColumn("locality", F.initcap("locality"))
                    .select(
                            F.col('circuitId').alias('circuit_id'),
                            F.col('circuitName').alias('circuit_name'),
                            F.col('lat').alias('latitude'),
                            F.col('long').alias('longitude'),
                            F.col('locality'),
                            F.col('country'),
                            F.col('Ingestion_timestamp'),
                            F.col('Source_file')))

# COMMAND ----------

# DBTITLE 1,Escribir df en silver
(df_circuits.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))
