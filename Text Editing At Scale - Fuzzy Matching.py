# Databricks notebook source
# MAGIC %md
# MAGIC ### Rules

# COMMAND ----------

# MAGIC %md
# MAGIC - Convert all upper-case letters to lower-case
# MAGIC - Replace spaces with an underscores: "Some Name" becomes some_name
# MAGIC - Separate two words that are joined with an underscore: "anotherName" becomes another_name
# MAGIC - Convert any character that is not a letter, digit, or underscore to underscore
# MAGIC - Convert multiple underscores to a single underscore
# MAGIC - For table names only, remove any underscore prefix

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Libraries

# COMMAND ----------

import re
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting all tables and columns by table

# COMMAND ----------

raw= spark.sql( '''SHOW TABLES IN schema''').toPandas()
raw_source= spark.sql( '''SHOW TABLES IN schema_souce''').toPandas()

# COMMAND ----------

def column_from_table(table_df,name):
  column_info = pd.DataFrame(columns=['col_name','data_type','table','database'])
  for index, row in table_df.iterrows():
    temp=spark.sql("desc {db}.{table}".format(db=row['database'],table= row['tableName'])).toPandas()
    temp['database']=row['database']
    temp["table"]=row['tableName']
    temp.drop('comment', axis=1, inplace=True)
    column_info=column_info.append(temp,ignore_index = True,sort=True)
  spark.createDataFrame(column_info).createOrReplaceTempView("{name}".format(name=name))
  return 
  

# COMMAND ----------

column_from_table(raw,'raw')
column_from_table(raw_source,'raw_source')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Here are the rows that match right from the get go
# MAGIC cache table overlap as
# MAGIC select 
# MAGIC   raw.col_name as alooma_name,
# MAGIC   raw.data_type as alooma_type,
# MAGIC   raw.table as alooma_table,
# MAGIC   raw.database as raw_database,
# MAGIC   raw_source.col_name as fivetran_name,
# MAGIC   raw_source.data_type as fivetran_type,
# MAGIC   raw_source.table as fivetran_table,
# MAGIC   raw_source.database as raw_source_database
# MAGIC from raw
# MAGIC inner join raw_source on raw_source.col_name =raw.col_name and raw_source.table=raw.table;
# MAGIC 
# MAGIC --Taking the matched rows out
# MAGIC cache table raw as
# MAGIC select * from raw
# MAGIC minus 
# MAGIC select 
# MAGIC alooma_name,
# MAGIC alooma_type,
# MAGIC alooma_table,
# MAGIC raw_database
# MAGIC from overlap;
# MAGIC --Taking the matched rows out
# MAGIC cache table raw_source as 
# MAGIC select 
# MAGIC   *
# MAGIC from raw_source
# MAGIC minus 
# MAGIC select 
# MAGIC   fivetran_name,
# MAGIC   fivetran_type,
# MAGIC   fivetran_table,
# MAGIC   raw_source_database
# MAGIC from overlap;

# COMMAND ----------

raw_source= spark.sql("select * from raw_source ").toPandas()
raw= spark.sql("select * from raw").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conversion Function
# MAGIC - Let's try some Fivetran naming conventions conversions and see if that gets us more matches

# COMMAND ----------

def column_conversion (df,column):
  df[column] = [re.sub(r'(?<!^)(?=[A-Z])', '_', str(value)).lower() for value in df[column]] # camel_case_split
  df[column]= [value.replace(' ','_') for value in df[column]] #space_to_underscore
  df[column]= [re.sub('[^0-9a-zA-Z]+', '_', value) for value in df[column]] # non_letter_digit
  return df[column]

# COMMAND ----------

raw['col_name']=column_conversion(raw,'col_name')
spark.createDataFrame(raw).createOrReplaceTempView("raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC cache table overlap_after_conversion as
# MAGIC select 
# MAGIC   raw.col_name as alooma_name,
# MAGIC   raw.data_type as alooma_type,
# MAGIC   raw.table as alooma_table,
# MAGIC   raw.database as raw_database,
# MAGIC   raw_source.col_name as fivetran_name,
# MAGIC   raw_source.data_type as fivetran_type,
# MAGIC   raw_source.table as fivetran_table,
# MAGIC   raw_source.database as raw_source_database
# MAGIC from raw
# MAGIC inner join raw_source on raw_source.col_name =raw.col_name and raw_source.table=raw.table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Taking out rows that are in the overlap

# COMMAND ----------

# MAGIC %sql
# MAGIC --Taking the matched rows out
# MAGIC cache table raw as
# MAGIC select * from raw
# MAGIC minus 
# MAGIC select 
# MAGIC alooma_name,
# MAGIC alooma_type,
# MAGIC alooma_table,
# MAGIC raw_database
# MAGIC from overlap_after_conversion;
# MAGIC --Taking the matched rows out
# MAGIC cache table raw_source as 
# MAGIC select 
# MAGIC   *
# MAGIC from raw_source
# MAGIC minus 
# MAGIC select 
# MAGIC   fivetran_name,
# MAGIC   fivetran_type,
# MAGIC   fivetran_table,
# MAGIC   raw_source_database
# MAGIC from overlap_after_conversion;

# COMMAND ----------

raw_source= spark.sql("select * from raw_source ").toPandas()
raw= spark.sql("select * from raw").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using fuzzy matching we will try and impute the new column names from the old

# COMMAND ----------

def checker(old_df,new_df):
  names_array=[]
  ratio_array=[]
  fivetran_table=[]
  for old_name in old_df['col_name']:
      temp=new_df[new_df['table'].isin(old_df[old_df['col_name']==old_name].table.unique())]
      if temp.empty ==True:
        names_array.append('no table match')
        ratio_array.append(0)
        fivetran_table.append('no table match')
      else:
        x=process.extractOne(old_name,temp['col_name'],scorer=fuzz.ratio)
        names_array.append(x[0])
        ratio_array.append(x[1])
        fivetran_table.append(temp['table'].unique()[0])
  df = pd.DataFrame()
  df['alooma_name']=pd.Series(old_df['col_name'])
  df['alooma_table']=pd.Series(old_df['table'])
  df['fivetran_name']=pd.Series(names_array)
  df['fivetran_table']=pd.Series(fivetran_table)
  df['correct_ratio']=pd.Series(ratio_array)
  return df

# COMMAND ----------

temp=raw_source[raw_source['table'].isin(raw[raw['col_name']=='partnership_id'].table.unique())]
temp['table'].unique()[0]

# COMMAND ----------

fuzzy_matching=checker(raw,raw_source)

# COMMAND ----------

fuzzy_matching.head()

# COMMAND ----------

fuzzy_matching=spark.createDataFrame(fuzzy_matching)
fuzzy_matching.createOrReplaceTempView("fuzzy_matching")

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from overlap
# MAGIC union 
# MAGIC select * from overlap_after_conversion
