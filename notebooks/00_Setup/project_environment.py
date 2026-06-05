# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS db_projects_ext_formula1
# MAGIC URL 'abfss://formula1@projectsadls.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `db_projects_sc`)
# MAGIC COMMENT 'External location for the formula1 container';

# COMMAND ----------

# MAGIC %fs ls 'abfss://formula1@projectsadls.dfs.core.windows.net/landing'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS formula1_incremental
# MAGIC    MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/' 
# MAGIC    COMMENT 'This is the main catalog for the formula1 incremental project';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incremental.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incremental.bronze
# MAGIC     MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/bronze';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incremental.silver
# MAGIC     MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/silver';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incremental.gold
# MAGIC     MANAGED LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/gold';   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME formula1_incremental.landing.raw
# MAGIC LOCATION 'abfss://formula1@projectsadls.dfs.core.windows.net/landing';
