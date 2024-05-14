# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieving Workspace Notebook Details Using the Python SDK
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC We'll be using the Databricks Python SDK to determine the number of notebooks in our workspace and to return some details about those notebooks such as the type of notebook, the location of the notebook in the Workspace (including remote repo git controlled notebooks) and when they were created or last modified.  A Databricks admin may wish to know this information to clean up the workspace periodically.  

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC Before we get started its always a good idea to ensure that you're using the most up to date version of the Databricks Python SDK.  The following code block upgrades the version on a Databricks compute resource.  

# COMMAND ----------

# DBTITLE 1,Upgrade the Databricks Python SDk
# MAGIC %pip install --upgrade databricks-sdk

# COMMAND ----------

# MAGIC %md
# MAGIC Note that you may have to restart the Python kernel for new libraries to be iniatized as shown in the following code block.  

# COMMAND ----------

# DBTITLE 1,Restart the Python kernel
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC Depending on the size of your workspace (based on number of notebooks) the generator that is returned by the Workspace Client may take a long time to retrieve all the results into a dictionary we'll eventually convert to a Spark dataframe.  We can limit the results returned using a Databricks Utilities text input widget for the path we'll like to review, with a default to the workpace root directory.    

# COMMAND ----------

# DBTITLE 1,Set Workspace Path to Review
dbutils.widgets.text("path", "/", "Workspace Path")

# COMMAND ----------

# MAGIC %md
# MAGIC *** 
# MAGIC Import the Workspace Client and the SQL service.  We won't be using the SQL service today, but it can be used to build queries for the resulting API calls.  
# MAGIC
# MAGIC Note that on a Databricks cluster, the WorkspaceClient class with automatically authenticate itself with the current workspace.  The access other workpsaces or to run the client outside of Databricks you'll need to authenticare with that workspace.  Please see the Python SDK documentation for more details:  
# MAGIC
# MAGIC [https://databricks-sdk-py.readthedocs.io/en/latest/index.html](https://databricks-sdk-py.readthedocs.io/en/latest/index.html)

# COMMAND ----------

# DBTITLE 1,Initialzie the Workspace Client
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,List Notebooks in Workspace
notebooks = w.workspace.list(path = dbutils.widgets.get("path"), recursive=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that *notebooks* is a generator type. Its lazy execution means that we haven't executed any API calls yet to the Databricks API.  

# COMMAND ----------

# DBTITLE 1,Note That The Result is a Generator Type
type(notebooks)

# COMMAND ----------

# MAGIC %md
# MAGIC Import required python libriries and functions.  

# COMMAND ----------

# DBTITLE 1,Import methods for Spark DF Conversion
import json
import pandas as pd
from pyspark.sql.functions import col, from_unixtime

# COMMAND ----------

# MAGIC %md
# MAGIC Turn the notebooks generator into a list of dictionaries, then convert to a pandas dataframe, and finally into a Spark dataframe.  

# COMMAND ----------

# DBTITLE 1,convert notebooks generator to pySpark dataframe
#convert returned notebooks generator to dicts
notebooks_as_dicts = [notebooks.as_dict() for notebooks in notebooks]

#create a pandas dataframe and then convert it to a pySpark dataframe
notebooks_pandas = pd.DataFrame(notebooks_as_dicts)
notebooks_df = spark.createDataFrame(notebooks_pandas)

# COMMAND ----------

# DBTITLE 1,Display Notebooks Dataframe
display(notebooks_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Next we'll format the created_at and modified_at Unix times as datetime stamps using the from_unixtime Spark SQL function.  

# COMMAND ----------


notebooks_df = (
  notebooks_df
  .withColumn("created_at", from_unixtime(col("created_at")/1e3, 'yyyy-MM-dd HH:mm:ss'))
  .withColumn("modified_at", from_unixtime(col("modified_at")/1e3, 'yyyy-MM-dd HH:mm:ss'))
)
display(notebooks_df.head(10))

# COMMAND ----------

# DBTITLE 1,Create TempView To Use SQL
notebooks_df.createOrReplaceTempView("tv_notebook_details")

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC Now that we have our dataframe registered as Spark temporary view, we can switch to SQL and perform some basic analysis on our notebooks.  

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(object_id) as tot_object_cnt 
# MAGIC from 
# MAGIC   tv_notebook_details
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   object_type
# MAGIC   ,count(object_id) as object_cnt
# MAGIC from 
# MAGIC   tv_notebook_details
# MAGIC group by
# MAGIC   object_type
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   language
# MAGIC   ,count(object_id) as object_cnt
# MAGIC from 
# MAGIC   tv_notebook_details
# MAGIC where 
# MAGIC   language is not null
# MAGIC group by
# MAGIC   language
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   language
# MAGIC   ,date(created_at) as created_at
# MAGIC   ,date(modified_at) as modified_at
# MAGIC   ,count(language) as cnt
# MAGIC from
# MAGIC   tv_notebook_details
# MAGIC where
# MAGIC   language is not null
# MAGIC group by
# MAGIC   1, 2, 3
# MAGIC ;
