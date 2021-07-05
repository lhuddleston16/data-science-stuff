# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook leverages spark to create a data profiler
# MAGIC - Several parameters such as dates, inspection columns, tables names, etc are used to define what should be looked at.
# MAGIC - Columns should be entered in the format 'col1,col2,col3...coln'
# MAGIC - A global temp view can be accessed outside of this notebook after process is ran.
# MAGIC - The following metrics are created:
# MAGIC  - number of rows with nulls and nans   
# MAGIC  - number of rows with white spaces (one or more space) or blanks
# MAGIC  - common statisitcs mode, mean, etc
# MAGIC  - number of distinct values in each column
# MAGIC  - most frequently occuring value in a column and its count
# MAGIC  - least frequently occuring value in a column and its count
# MAGIC  - max and min values in each column

# COMMAND ----------

# MAGIC %md
# MAGIC - Setting up widgets and getting their values

# COMMAND ----------

dbutils.widgets.text("1.) Start Date","")
dbutils.widgets.text("2.) End Date","")
dbutils.widgets.text("Table", "")
dbutils.widgets.text("Schema", "")
dbutils.widgets.text("Columns", "")
dbutils.widgets.text("date_column", "")

# COMMAND ----------

start_date=dbutils.widgets.get("1.) Start Date")
end_date=dbutils.widgets.get("2.) End Date")
table=dbutils.widgets.get("Table")
schema=dbutils.widgets.get("Schema")
columns=dbutils.widgets.get("Columns")
date_column=dbutils.widgets.get("date_column")

# COMMAND ----------

# MAGIC %md
# MAGIC - Removal of widgets.Only used when you need to recreate a widget

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC - Table and columns to evaluate

# COMMAND ----------

df=spark.sql("""select {columns} from {schema}.{table} where {date_column} between last_day('{start_date}') and last_day('{end_date}')""".format(columns=columns,schema=schema,table=table,date_column=date_column,start_date=start_date,end_date=end_date))

# COMMAND ----------

# MAGIC %md
# MAGIC - Defining Functions

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
spark.conf.set("spark.sql.execution.arrow.enabled", "false") # We do not have an updated version so it fails unless we make sure this is false

# COMMAND ----------

def get_dtype(df,colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]

# COMMAND ----------


def dataprofile(data_all_df,data_cols):
    data_df = data_all_df.select(data_cols)
    columns2Bprofiled = data_df.columns
    global schema_name, table_name
    if not 'schema_name' in globals():
        schema_name = 'schema_name'
    if not 'table_name' in globals():
        table_name = 'table_name' 
    dprof_df = pd.DataFrame({'column_names':data_df.columns,\
                             'data_types':[x[1] for x in data_df.dtypes]}) 
    dprof_df = dprof_df[['column_names', 'data_types']]
    #dprof_df.set_index('column_names', inplace=True, drop=False)
    # ======================
    num_rows = data_df.count()
    dprof_df['num_rows'] = num_rows
    # ======================    
    # number of rows with nulls and nans   
    df_nacounts = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns \
                                  if data_df.select(c).dtypes[0][1]not in ['timestamp','date']]).toPandas().transpose()
    df_nacounts = df_nacounts.reset_index()  
    df_nacounts.columns = ['column_names','num_null']
    dprof_df = pd.merge(dprof_df, df_nacounts, on = ['column_names'], how = 'left')
    # ========================
    # number of rows with white spaces (one or more space) or blanks
    num_spaces = [data_df.where(F.col(c).rlike('^\\s+$')).count() for c in data_df.columns]
    dprof_df['num_spaces'] = num_spaces
    num_blank = [data_df.where(F.col(c)=='').count() for c in data_df.columns]
    dprof_df['num_blank'] = num_blank
    # =========================
    # using the in built describe() function 
    desc_df = data_df.describe().toPandas().transpose()
    desc_df.columns = ['count', 'mean', 'stddev', 'min', 'max']
    desc_df = desc_df.iloc[1:,:]  
    desc_df = desc_df.reset_index()  
    desc_df.columns.values[0] = 'column_names'  
    desc_df = desc_df[['column_names','count', 'mean', 'stddev']] 
    dprof_df = pd.merge(dprof_df, desc_df , on = ['column_names'], how = 'left')
    # ===========================================
    # number of distinct values in each column
    dprof_df['num_distinct'] = [data_df.select(x).distinct().count() for x in columns2Bprofiled]
    # ============================================
    # most frequently occuring value in a column and its count
    dprof_df['most_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=False).limit(1).\
                                       toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['most_freq_value'] = [x[0] for x in dprof_df['most_freq_valwcount']]
    dprof_df['most_freq_value_count'] = [x[1] for x in dprof_df['most_freq_valwcount']]
    dprof_df = dprof_df.drop(['most_freq_valwcount'],axis=1)
    # least frequently occuring value in a column and its count
    dprof_df['least_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=True).limit(1).\
                                        toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['least_freq_value'] = [x[0] for x in dprof_df['least_freq_valwcount']]
    dprof_df['least_freq_value_count'] = [x[1] for x in dprof_df['least_freq_valwcount']]
    dprof_df = dprof_df.drop(['least_freq_valwcount'],axis=1)
    # ============================================
    # max and min values in each column
    dprof_df['max_value']= [data_df.agg({x: "max"}).collect()[0][0] if get_dtype(data_df,x) != 'string' else None for x in columns2Bprofiled]
    dprof_df['max_value_count']=[data_df.where(col(x) == data_df.agg({x: "max"}).collect()[0][0]).count() if get_dtype(data_df,x) != 'string' else None for x in columns2Bprofiled]
    dprof_df['min_value']= [data_df.agg({x: "min"}).collect()[0][0] if get_dtype(data_df,x) != 'string' else None for x in columns2Bprofiled]
    dprof_df['min_value_count']=[data_df.where(col(x) == data_df.agg({x: "min"}).collect()[0][0]).count() if get_dtype(data_df,x) != 'string' else None for x in columns2Bprofiled]
    dprof_df.fillna(value=pd.np.nan, inplace=True)
    dprof_df.fillna('', inplace=True)
    dprof_df = dprof_df.astype(str)
    dprof_df=spark.createDataFrame(dprof_df)
    return dprof_df

# COMMAND ----------

output=dataprofile(df,df.columns)

# COMMAND ----------

output.createOrReplaceTempView("QA_focus_columns")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW QA_focus_columns_temp AS 
# MAGIC SELECT * FROM QA_focus_columns;
