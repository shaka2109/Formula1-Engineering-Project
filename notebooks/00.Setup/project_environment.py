# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS db_projects_ext_formula1
# MAGIC URL 'abfss://formula1@projectsadls.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `db_projects_sc`)
# MAGIC COMMENT 'External location for the formula1 container';

# COMMAND ----------

# MAGIC %fs ls 'abfss://formula1@projectsadls.dfs.core.windows.net/landing'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS formula1
# MAGIC    MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/' 
# MAGIC    COMMENT 'This is the main catalog for the formula1 project' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.bronze
# MAGIC     MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/bronze';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.silver
# MAGIC     MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/silver';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.gold
# MAGIC     MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/gold';   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME formula1.landing.files
# MAGIC LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/landing';
