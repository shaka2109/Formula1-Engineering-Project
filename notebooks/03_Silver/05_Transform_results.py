# Databricks notebook source
# MAGIC %run ../01_Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %run ../01_Miscellaneous/Support_functions

# COMMAND ----------

# DBTITLE 1,Dinamico
table_name = 'results'
source_table = f'{catalog_name}.{bronze_schema}.{table_name}'
table_path = f'{catalog_name}.{silver_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Leer tabla delta como dataframe
df = spark.read.table(source_table)

# COMMAND ----------

# DBTITLE 1,Limpiar dataframe
from pyspark.sql import functions as F

df_results = ((df.filter(df.season.isNotNull() 
                        & df.round.isNotNull() 
                        & df.constructorId.isNotNull()
                        & df.driverId.isNotNull()))
                .dropDuplicates(['season', 'round', 'constructorId', 'driverId'])
                .withColumn("raceName", F.initcap("raceName"))
                .select(
                            F.col('date').alias('race_date'),
                            F.col('raceName').alias('race_name'),
                            F.col('round'),
                            F.col('season'),
                            F.col('constructorId').alias('constructor_id'),
                            F.col('driverId').alias('driver_id'),
                            F.col('grid').alias('grid_position'),
                            F.col('laps').alias('completed_laps'),
                            F.col('number').alias('car_number'),
                            F.col('points'),
                            F.col('position').alias('finish_position'),
                            F.col('positionText').alias('finish_position_text'),
                            F.col('status'),
                            F.col('Ingestion_timestamp').alias('ingestion_timestamp'),
                            F.col('Source_file').alias('source_file')))

# COMMAND ----------

# DBTITLE 1,Escribir df en silver
(df_results.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))

# COMMAND ----------

display(spark.table(table_path))
