# Databricks notebook source
# MAGIC %run ../01.Miscellaneous/Env_configuration

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

table_path = f'{catalog_name}.{gold_schema}.dim_races'

# COMMAND ----------

# DBTITLE 1,Tabla 1
df_races = spark.read.table(f'{catalog_name}.{silver_schema}.races')
df_circuits = spark.read.table(f'{catalog_name}.{silver_schema}.circuits')

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

(df_dim_races.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))
