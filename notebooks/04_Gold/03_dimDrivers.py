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

join_key='driver_id'
table_path = f'{catalog_name}.{gold_schema}.dim_drivers'

# COMMAND ----------

# DBTITLE 1,Tabla 1
df_drivers = (spark.read.table(f'{catalog_name}.{silver_schema}.drivers')
                            .filter(F.col('batch_id')==v_batch_id))
df_nationality = spark.read.table(f'{catalog_name}.{gold_schema}.ref_nationality')

# COMMAND ----------

# DBTITLE 1,Join
df_dim_drivers = (df_drivers.join(df_nationality, df_drivers.nationality == df_nationality.nationality,'inner')
                            .select(
                                df_drivers.driver_id,
                                df_drivers.driver_name,
                                df_drivers.date_of_birth,
                                df_drivers.nationality,
                                df_nationality.region
                            ))

# COMMAND ----------

write_to_gold(df_dim_drivers, table_path, join_key)
