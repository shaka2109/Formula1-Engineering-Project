# Databricks notebook source
# BRONZE TABLES FUNCTIONS

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Leer archivos CSV

def read_csv_file(source_file,myschema):
    file_read = (spark.read.format('csv')
                     .option('header', True)
                     .schema(myschema)
                     .option('mode', 'failfast')
                     .load(source_file))
    return file_read

# COMMAND ----------

# Leer archivos JSON

def read_json_file(source_file,myschema):
    file_read = (spark.read.format('json')
                          .schema(myschema)
                          .option('mode', 'failfast')
                          .load(source_file))
    return file_read

# COMMAND ----------

# Leer archivos multiline JSON

def read_json_multiline_file(source_file,myschema):
    file_read = (spark.read.format('json')
                          .schema(myschema)
                          .option('multiline', True)
                          .option('mode', 'failfast')
                          .load(source_file))
    return file_read

# COMMAND ----------

# Crear columnas para auditar

from pyspark.sql.functions import col, current_timestamp

def add_ingestion_metadata(df):
    final_df = (df.withColumn("Ingestion_timestamp", current_timestamp())
                  .withColumn("Source_file", col("_metadata.file_path")))
    return final_df

# COMMAND ----------

# Escribir delta tables

from pyspark.sql import functions as F

def write_delta(df,table_path,batch_id):
    
    final_df = df.withColumn('batch_id', F.lit(batch_id))
    
    (final_df.write.format('delta')
             .mode('overwrite')
             .partitionBy('batch_id')
             .option('replaceWhen', f"batch_id = '{batch_id}'")
             .saveAsTable(table_path))

# COMMAND ----------

# SILVER TABLES FUNCTIONS

# COMMAND ----------

# Incremental Load (MERGE)
from delta.tables import DeltaTable

def write_to_silver(source, target, join_key):

    columns_to_update = source.columns
    columns_to_update.remove(join_key)

    df_final = (source.withColumn("created_timestamp", F.current_timestamp())
                      .withColumn("updated_timestamp", F.current_timestamp()))

    if not spark.catalog.tableExists(target):
        (df_final.write.format('delta')
                    .mode('overwrite')
                    .saveAsTable(target))
    else:
        delta_table = DeltaTable.forName(spark, target)                           # Target table
        updated_map = {column: f's.{column}' for column in columns_to_update}
        updated_map['updated_timestamp']='s.updated_timestamp'

        (
            delta_table.alias('t').merge(df_final.alias('s'),f't.{join_key}=s.{join_key}')
            .whenMatchedUpdate(
                condition='s.batch_id>=t.batch_id',
                set = updated_map
            )
            .whenNotMatchedInsertAll()
            .execute()
        )


# COMMAND ----------

# Incremental Load (MERGE)
from delta.tables import DeltaTable

def write_to_silver_multi_keys(source, target, join_keys, columns_to_update):

    df_final = (source.withColumn("created_timestamp", F.current_timestamp())
                      .withColumn("updated_timestamp", F.current_timestamp()))

    if not spark.catalog.tableExists(target):
        (df_final.write.format('delta')
                    .mode('overwrite')
                    .saveAsTable(target))
    else:
        delta_table = DeltaTable.forName(spark, target)                           # Target table
        updated_map = {column: f's.{column}' for column in columns_to_update}
        updated_map['updated_timestamp']='s.updated_timestamp'

        (
            delta_table.alias('t').merge(df_final.alias('s'),join_keys)
            .whenMatchedUpdate(
                condition='s.batch_id>=t.batch_id',
                set = updated_map
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# COMMAND ----------

# GOLD TABLES FUNCTIONS

# COMMAND ----------

# Incremental Load (MERGE)
from delta.tables import DeltaTable

def write_to_gold(source, target, join_key):

    columns_to_update = source.columns
    columns_to_update.remove(join_key)

    df_final = (source.withColumn("created_timestamp", F.current_timestamp())
                      .withColumn("updated_timestamp", F.current_timestamp()))

    if not spark.catalog.tableExists(target):
        (df_final.write.format('delta')
                    .mode('overwrite')
                    .saveAsTable(target))
    else:
        delta_table = DeltaTable.forName(spark, target)                           # Target table
        updated_map = {column: f's.{column}' for column in columns_to_update}
        updated_map['updated_timestamp']='s.updated_timestamp'

        (
            delta_table.alias('t').merge(df_final.alias('s'),f't.{join_key}=s.{join_key}')
            .whenMatchedUpdate(
                set = updated_map
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# COMMAND ----------

# Incremental Load (MERGE)
from delta.tables import DeltaTable

def write_to_gold_multi_keys(source, target, join_keys, columns_to_update):

    df_final = (source.withColumn("created_timestamp", F.current_timestamp())
                      .withColumn("updated_timestamp", F.current_timestamp()))

    if not spark.catalog.tableExists(target):
        (df_final.write.format('delta')
                    .mode('overwrite')
                    .saveAsTable(target))
    else:
        delta_table = DeltaTable.forName(spark, target)                           # Target table
        updated_map = {column: f's.{column}' for column in columns_to_update}
        updated_map['updated_timestamp']='s.updated_timestamp'

        (
            delta_table.alias('t').merge(df_final.alias('s'),join_keys)
            .whenMatchedUpdate(
                set = updated_map
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# COMMAND ----------

# Revisar keys

# spark.table(target) \
#    .groupBy("race_date", "driver_id", "constructor_id") \
#    .count() \
#    .filter("count > 1") \
#    .show()
