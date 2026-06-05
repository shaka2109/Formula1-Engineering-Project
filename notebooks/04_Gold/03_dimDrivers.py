# Databricks notebook source
# MAGIC %run ../01_Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Se crea una tabla que ayude al analisis geografico (ref_nationality_region) para constructores y drivers.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

table_path = f'{catalog_name}.{gold_schema}.dim_drivers'

# COMMAND ----------

# DBTITLE 1,Tabla 1
df_drivers = spark.read.table(f'{catalog_name}.{silver_schema}.drivers')
df_nationality = spark.read.table(f'{catalog_name}.{gold_schema}.ref_nationality_region')

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

(df_dim_drivers.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))
