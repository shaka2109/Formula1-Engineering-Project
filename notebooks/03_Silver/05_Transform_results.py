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
table_name = 'results'
join_keys = ('t.season=s.season and t.round=s.round and t.constructor_id=s.constructor_id and t.driver_id=s.driver_id')
join_list = ['race_date','constructor_id','driver_id']
source_table = f'{catalog_name}.{bronze_schema}.{table_name}'
table_path = f'{catalog_name}.{silver_schema}.{table_name}'

# COMMAND ----------

# DBTITLE 1,Leer tabla delta como dataframe
df = spark.read.table(source_table).filter(F.col('batch_id')==v_batch_id)

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
                            F.col('Source_file').alias('source_file'),
                            F.col('batch_id')))

# COMMAND ----------

    table_columns = df_results.columns
    columns_to_update = [x for x in table_columns if x not in join_list]

# COMMAND ----------

write_to_silver_multi_keys(df_results, table_path, join_keys, columns_to_update)

# COMMAND ----------

# display(spark.table(table_path))
