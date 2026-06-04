# Databricks notebook source
# Crear columnas para auditar

from pyspark.sql.functions import col, current_timestamp

def add_ingestion_metadata(df):
    final_df = (df.withColumn("Ingestion_timestamp", current_timestamp())
                  .withColumn("Source_file", col("_metadata.file_path")))
    return final_df

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

# Escribir archivos CSV

def write_file(df,table_path):
    (df.write.format('delta')
             .mode('overwrite')
             .saveAsTable(table_path))

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
