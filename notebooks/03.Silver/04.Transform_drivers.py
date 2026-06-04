# Databricks notebook source
# MAGIC %run ../01.Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01.Miscellaneous/Support_functions

# COMMAND ----------

# DBTITLE 1,Dinamico
table_name = 'drivers'
source_table = f'{catalog_name}.{bronze_schema}.{table_name}'
table_path = f'{catalog_name}.{silver_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Leer tabla delta como dataframe
df = spark.read.table(source_table)

# COMMAND ----------

# DBTITLE 1,Limpiar dataframe
from pyspark.sql import functions as F

df_drivers = (df.dropDuplicates()
                    .withColumn("driver_name", F.concat_ws(' ','name.givenName', 'name.familyName'))
                    .withColumn("nationality", F.initcap("nationality"))
                    .withColumn("driver_name", F.initcap("driver_name"))
                    .select(
                            F.col('driverId').alias('driver_id'),
                            F.col('driver_name'),
                            F.col('dateOfBirth').alias('date_of_birth'),
                            F.col('nationality'),
                            F.col('Ingestion_timestamp').alias('ingestion_timestamp'),
                            F.col('Source_file').alias('source_file')))

# COMMAND ----------

# DBTITLE 1,Escribir df en silver
(df_drivers.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))

# COMMAND ----------

display(spark.table(table_path))
