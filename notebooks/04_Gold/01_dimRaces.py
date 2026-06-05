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

join_key='race_date'
table_path = f'{catalog_name}.{gold_schema}.dim_races'

# COMMAND ----------

# DBTITLE 1,Tabla 1
df_circuits = (spark.read.table(f'{catalog_name}.{silver_schema}.circuits')
                            .filter(F.col('batch_id')==v_batch_id))
df_races = (spark.read.table(f'{catalog_name}.{silver_schema}.races')
                            .filter(F.col('batch_id')==v_batch_id))

# COMMAND ----------

# DBTITLE 1,Join
df_dim_races = (df_races.join(df_circuits, df_races.circuit_id == df_circuits.circuit_id,'inner')
                            .select(
                                df_races.season,
                                df_races.round,
                                df_races.race_name,
                                df_races.race_date,
                                df_circuits.circuit_name,
                                df_circuits.locality,
                                df_circuits.country
                            ))

# COMMAND ----------

write_to_gold(df_dim_races, table_path, join_key)
