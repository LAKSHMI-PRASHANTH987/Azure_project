# Databricks notebook source
df = spark.read.format("parquet")\
        .load("abfss://bronze@storageaccountprojectlp.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##**AUTOLOADER**
# MAGIC

# COMMAND ----------

df_user = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("abfss://bronze@storageaccountprojectlp.dfs.core.windows.net/DimUser")
)

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/checkpoint", recurse=True)
display(df_user, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/checkpoint")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_user = df_user.withColumn("user_name",upper(col("user_name")))

# Step 1 — delete
dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/display_checkpoint", recurse=True)


display(df_user, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/display_checkpoint")

# COMMAND ----------

display(df_user, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/display_checkpoint")

# COMMAND ----------

import os
import sys

project_pth = os.path.join(os.getcwd(),'..','..')


sys.path.append(project_pth)


# COMMAND ----------

from utils.transformation import reusable

# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/display_checkpoint", recurse=True)


display(df_user, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/display_checkpoint")

# COMMAND ----------

df_user = df_user.dropDuplicates(['user_id'])

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/display_checkpoint", recurse=True)


display(df_user, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/display_checkpoint")


# COMMAND ----------

#df_user = df_user.coalesce(1)
dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/checkpoint", recurse=True)


df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimUser/data")\
    .toTable("spotifycata.silver.DimUser")

# COMMAND ----------



# COMMAND ----------

df_art = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("abfss://bronze@storageaccountprojectlp.dfs.core.windows.net/DimArtist")
)

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/display_checkpoint", recurse=True)


display(df_art, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/display_checkpoint")


# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art,['_rescued_data'])

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/display_checkpoint", recurse=True)


display(df_art, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/display_checkpoint")

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/checkpoint", recurse=True)


df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotifycata.silver.DimArtist")

# COMMAND ----------

df_track = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimTrack/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("abfss://bronze@storageaccountprojectlp.dfs.core.windows.net/DimTrack")
)

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimTrack/checkpoint", recurse=True)

df_track = df_track.withColumn("durationFlag",when(col('duration_sec')<150,"low")\
                                             .when(col('duration_sec')<300,"medium")\
                                             .otherwise("high"))

df_track = df_track.withColumn("track_name",regexp_replace(col('track_name'),'-',' '))

    
    
    

# COMMAND ----------

# Step 1 — delete checkpoint
dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimTrack/display_checkpoint", recurse=True)

# Step 2 — display WITH checkpoint location
display(df_track, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimTrack/display_checkpoint")

# COMMAND ----------

df_track = reusable().dropColumns(df_track,['_rescued_Data'])

df_track = df_track.dropDuplicates(['track_id'])

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimTrack/checkpoint", recurse=True)


df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotifycata.silver.DimTrack")

# COMMAND ----------

df_date = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimDate/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("abfss://bronze@storageaccountprojectlp.dfs.core.windows.net/DimDate")
)

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimDate/checkpoint", recurse=True)

df_date = reusable().dropColumns(df_date,['_rescued_data'])

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/DimDate/data")\
    .toTable("spotifycata.silver.DimDate")

# COMMAND ----------

df_fact = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageaccountprojectlp.dfs.core.windows.net/FactStream/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("abfss://bronze@storageaccountprojectlp.dfs.core.windows.net/FactStream")
)

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/FactStream/checkpoint", recurse=True)

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])


# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageaccountprojectlp.dfs.core.windows.net/FactStream/data")\
    .toTable("spotifycata.silver.FactStream")

# COMMAND ----------

dbutils.fs.rm("abfss://silver@storageaccountprojectlp.dfs.core.windows.net/FactStream/display_checkpoint", recurse=True)


display(df_fact, checkpointLocation="abfss://silver@storageaccountprojectlp.dfs.core.windows.net/FactStream/display_checkpoint")