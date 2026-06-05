# Databricks notebook source
# MAGIC %run ../01_Miscellaneous/Env_configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Se crea una tabla que ayude al analisis geografico (ref_nationality_region) para constructores y drivers.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

table_path = f'{catalog_name}.{gold_schema}.dim_constructors'

# COMMAND ----------

# DBTITLE 1,Tabla 1
df_constructors = spark.read.table(f'{catalog_name}.{silver_schema}.constructors')
df_nationality = spark.read.table(f'{catalog_name}.{gold_schema}.ref_nationality_region')

# COMMAND ----------

# DBTITLE 1,Join
df_dim_constructors = (df_constructors.join(df_nationality, df_constructors.nationality == df_nationality.nationality,'inner')
                            .select(
                                df_constructors.constructor_id,
                                df_constructors.constructor_name,
                                df_constructors.nationality,
                                df_nationality.region
                            ))

# COMMAND ----------

(df_dim_constructors.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))
