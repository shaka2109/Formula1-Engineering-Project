# Databricks notebook source
# MAGIC %run ../01.Miscellaneous/Env_configuration

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

table_path = f'{catalog_name}.{gold_schema}.fact_results'

# COMMAND ----------

df_results = (spark.read.table(f'{catalog_name}.{silver_schema}.results')
                            .withColumn('session_type', F.lit('RACE'))
                            .drop('race_date','race_name','ingestion_timestamp','source_file'))

# COMMAND ----------

df_sprints = (spark.read.table(f'{catalog_name}.{silver_schema}.sprints')
                            .withColumn('session_type', F.lit('SPRINT'))
                            .drop('sprint_date','race_name','ingestion_timestamp','source_file'))

# COMMAND ----------

df_union = df_results.unionByName(df_sprints)

# COMMAND ----------

df_union_final = (df_union.withColumn('is_win',F.col('finish_position')==1)
                         .withColumn('is_podium',F.col('finish_position').between(1,3))
                         .withColumn('has_points',F.col('points')>0))

# COMMAND ----------

(df_union_final.write.format('delta')
             .mode('overwrite')
             .option("overwriteSchema", "true")
             .saveAsTable(table_path))
