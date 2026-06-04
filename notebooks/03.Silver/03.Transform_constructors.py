# Databricks notebook source
# MAGIC %run ../01.Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01.Miscellaneous/Support_functions

# COMMAND ----------

# DBTITLE 1,Dinamico
table_name = 'constructors'
source_table = f'{catalog_name}.{bronze_schema}.{table_name}'
table_path = f'{catalog_name}.{silver_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Leer tabla delta como dataframe
df = spark.read.table(source_table)

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
                            F.col('Source_file').alias('source_file')))

# COMMAND ----------

# DBTITLE 1,Escribir df en silver
(df_constructors.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))

# COMMAND ----------

display(spark.table(table_path))
