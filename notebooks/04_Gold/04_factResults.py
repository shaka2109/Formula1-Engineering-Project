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

join_keys=('t.season=s.season and t.round=s.round and t.constructor_id=s.constructor_id and t.driver_id=s.driver_id and t.session_type=s.session_type')
join_list=['season','round','driver_id','constructor_id','session_type']
table_path = f'{catalog_name}.{gold_schema}.fact_results'

# COMMAND ----------

df_results = (spark.read.table(f'{catalog_name}.{silver_schema}.results')
                            .withColumn('session_type', F.lit('RACE'))
                            .drop('race_date','race_name','ingestion_timestamp','source_file','batch_id','created_timestamp','updated_timestamp'))

# COMMAND ----------

df_sprints = (spark.read.table(f'{catalog_name}.{silver_schema}.sprints')
                            .withColumn('session_type', F.lit('SPRINT'))
                            .drop('sprint_date','race_name','ingestion_timestamp','source_file','batch_id','created_timestamp','updated_timestamp'))

# COMMAND ----------

df_union = df_results.unionByName(df_sprints)

# COMMAND ----------

df_union_final = (df_union.withColumn('is_win',F.col('finish_position')==1)
                         .withColumn('is_podium',F.col('finish_position').between(1,3))
                         .withColumn('has_points',F.col('points')>0))

# COMMAND ----------

    table_columns = df_union_final.columns
    columns_to_update = [x for x in table_columns if x not in join_list]

# COMMAND ----------

write_to_gold_multi_keys(df_union_final, table_path, join_keys, columns_to_update)
