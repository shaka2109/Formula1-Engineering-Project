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
table_name = 'circuits'
join_key = 'circuit_id'
source_table = f'{catalog_name}.{bronze_schema}.{table_name}'
table_path = f'{catalog_name}.{silver_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Leer tabla delta como dataframe
df = spark.read.table(source_table).filter(F.col('batch_id')==v_batch_id)

# COMMAND ----------

# DBTITLE 1,Limpiar dataframe
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
                            F.col('Source_file'),
                            F.col('batch_id')))

# COMMAND ----------

# DBTITLE 1,Escribir df en silver
write_to_silver(df_circuits, table_path, join_key)
