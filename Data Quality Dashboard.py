# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality 
# MAGIC - Runs a data profiler to check for deviations 
# MAGIC - Runs a list of SQL tests to check for failures
# MAGIC - Plots failures
# MAGIC - Runs tests in parallel 

# COMMAND ----------

dbutils.widgets.text("1.) Start Date", "2015-01-01")
dbutils.widgets.text("2.) End Date", "2018-01-01")
dbutils.widgets.dropdown("3.) Schema", "prod", ["prod","dev",'define my own'])
dbutils.widgets.dropdown("4.) Refresh SQL tests?", "No", ["Yes","No"])
dbutils.widgets.dropdown("5.) Focus?", "None", ['financial','usage','sentiment','engagement','None'])

#Getting widget values
start_date=dbutils.widgets.get("1.) Start Date")
end_date=dbutils.widgets.get("2.) End Date")
schema=dbutils.widgets.get("3.) Schema")
refresh_indicator=dbutils.widgets.get("4.) Refresh SQL tests?")
focus=dbutils.widgets.get("5.) Focus?")

if schema in ['prod','dev']:
  financial_schema=schema
  usage_schema=schema
  sentiment_schema=schema
  engagement_schema=schema
else:
  #Define your own schema here
  financial_schema='dev'
  usage_schema='dev'
  sentiment_schema='dev'
  engagement_schema='dev'

#Update your tables and columns here when needed.
import pandas as pd
input_list = [['index_',Finance_DB_schema,'date_month','product_','sfdc_fam_id',start_date,end_date,'Finance_DB'],
              ['finance_index_',financial_schema,'month_end','id','fam_id',start_date,end_date,'financial'], 
              ['usage_index',usage_schema,'month_start','id','fam_id',start_date,end_date,'usage'],
              ['sentiment_index',sentiment_schema,'month_end','id','fam_id',start_date,end_date,'sentiment'],
              ['engagement_index',engagement_schema,'date_month','id','fam_id',start_date,end_date,'engagement']] 
    
input_table = pd.DataFrame(input_list, columns =['table', 'schema','date_column','revenue_column','fam_column','start_date','end_date','table_type']) 

#Defining the dynamic input
if focus != 'None':
  focus_table=input_table[input_table['table_type']==focus]['table'].values[0]
  focus_schema=input_table[input_table['table_type']==focus]['schema'].values[0]
  focus_date=input_table[input_table['table_type']==focus]['date_column'].values[0]
  focus_columns=spark.sql("""select * from {focus_schema}.{focus_table}""".format(focus_schema=focus_schema,focus_table=focus_table)).columns
else:
  focus_table="None"
  focus_schema="None"
  focus_date="None"
  focus_columns=['No focus selected']
dbutils.widgets.multiselect("6.) Focus column?", focus_columns[0], focus_columns)
focus_column_selected=dbutils.widgets.get("6.) Focus column?")


# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from multiprocessing.pool import ThreadPool
spark.conf.set("spark.sql.execution.revenueow.enabled", "false") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating an input table that can be used across notebooks

# COMMAND ----------

input_table=spark.createDataFrame(input_table)
input_table.createOrReplaceTempView("input_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW input_table AS 
# MAGIC SELECT * FROM input_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running notebooks in paralell

# COMMAND ----------


# These notebooks will always run
notebooks2run=["/QA_total_fam_revenue","/QA_revenue_per_fam"]

# Function to add notebooks
def add_when_true(indicator,path):
  if indicator ==True and path not in notebooks2run:
    notebooks2run.append(path)
    
# Checking which notebooks should be active
add_sql_notebook_indicator=refresh_indicator =='Yes'
add_columns_notebook_indicator=focus!='None'

#Adding notebooks if true/active
add_when_true(add_sql_notebook_indicator,'/QA_sql_tests')
add_when_true(add_columns_notebook_indicator,'/QA_focus_columns')

# COMMAND ----------

pool=ThreadPool(len(notebooks2run))
QA_total_fam_revenue_failures,QA_revenue_per_fam_failures= pool.map(
  lambda path: dbutils.notebook.run(
  "/GTMSO/QA/Table_Checks"+path,
  timeout_seconds=1000,
  arguments={"1.) Start Date": start_date,
             "2.) End Date":end_date,
             "Columns":focus_column_selected,
             "Table":focus_table,
             "Schema":focus_schema,
             "date_column":focus_date}),
  notebooks2run)[0:2]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total revenue and Family Checks

# COMMAND ----------

#raise error if any of the indexes do not match Finance_DB for total revenue or number of families
if QA_total_fam_revenue_failures != '0':
  displayHTML("""<font size="3" color="red" face="sans-serif">The total revenue and/or number of families does not match between the indexes and Finance_DB for {} month(s).""".format(QA_total_fam_revenue_failures))
else:
  displayHTML("""<font size="3" color="green" face="sans-serif">Total revenue and family count matches between the indexes and Finance_DB""")

# COMMAND ----------

from plotly.offline import plot
from plotly.graph_objs import *
df_revenue_fam=spark.sql('''select * from global_temp.QA_total_fam_revenue_temp where date_month < current_date()''').toPandas()

# COMMAND ----------

flag_graph = plot(
  [
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['revenue_FLAG'],name="revenue"),
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['FAM_FLAG'],name="Family")
  ],
  output_type='div'
)

displayHTML(flag_graph)

# COMMAND ----------

revenue_graph = plot(
  [
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['financial_revenue'],name="financial"),
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['usage_revenue'],name="usage"),
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['sentiment_revenue'],name="sentiment"),
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['engagement_revenue'],name="engagement")
  ],
  output_type='div'
)

displayHTML(revenue_graph)

# COMMAND ----------

fam_graph = plot(
  [
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['financial_n_fam'],name="financial"),
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['usage_n_fam'],name="usage"),
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['sentiment_n_fam'],name="sentiment"),
    Scatter(x=df_revenue_fam['date_month'], y=df_revenue_fam['engagement_n_fam'],name="engagement")
  ],
  output_type='div'
)

displayHTML(fam_graph)

# COMMAND ----------

# MAGIC %md
# MAGIC ## revenue Per Family Matches Finance_DB

# COMMAND ----------

#raise error if any of the indexes do not match Finance_DB for total revenue or number of families
if QA_revenue_per_fam_failures != '0':
  displayHTML("""<font size="3" color="red" face="sans-serif">Family revenue does not match between the indexes and Finance_DB for {} family ID(s).""".format(QA_revenue_per_fam_failures))
else:
  displayHTML("""<font size="3" color="green" face="sans-serif">Each family revenue matches across indexes.""")

# COMMAND ----------

df_per_fam=spark.sql('''select * from global_temp.QA_revenue_per_fam_temp where date_month < current_date()''').toPandas()

# COMMAND ----------

per_fam_graph = plot(
  [
    Scatter(x=df_per_fam['date_month'], y=df_per_fam['num_flag'],name="Number of Familes")
  ],
  output_type='div'
)

displayHTML(per_fam_graph)

# COMMAND ----------

if QA_revenue_per_fam_failures == '0':
  displayHTML("""<font size="5"> No failed family IDs to display""")
else:
  display(spark.sql("select * from global_temp.QA_revenue_per_fam_failures_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Evaluation

# COMMAND ----------

if focus=='None':
  displayHTML("""<font size="5"> No columns to display""")
else:
  display(spark.sql("select * from global_temp.QA_focus_columns_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL tests
# MAGIC - Raw table populated
# MAGIC - Multiple account owners

# COMMAND ----------

# MAGIC %sql 
# MAGIC refresh prod.QA_sql_test_failures

# COMMAND ----------

QA_sql_tests_failures=spark.sql("""select * from prod.QA_sql_test_failures""").count()

# COMMAND ----------

if QA_sql_tests_failures != 0:
  displayHTML("""<font size="3" color="red" face="sans-serif">{} SQL test(s) did not pass.""".format(QA_sql_tests_failures))
else:
  displayHTML("""<font size="3" color="green" face="sans-serif">All SQL tests have passed.""")


# COMMAND ----------

all_failures=sum([int(QA_total_fam_revenue_failures),int(QA_revenue_per_fam_failures),int(QA_sql_tests_failures)])
if all_failures ==0:
  dbutils.notebook.exit('Success')
else:
  dbutils.notebook.exit('Failure')
