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

# COMMAND ----------

# DBTITLE 1,Initialzie the Workspace Client
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
wc = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,List Notebooks in Workspace
notebooks = wc.workspace.list(path = dbutils.widgets.get("path"), recursive=True)

# COMMAND ----------

# DBTITLE 1,Note That The Result is a Generator Type
type(notebooks)

# COMMAND ----------

import json
import pandas as pd
from pyspark.sql.functions import lit

# COMMAND ----------

#convert returned notebooks generator to dicts
notebooks_as_dicts = [notebooks.as_dict() for notebooks in notebooks]

#create a pandas dataframe and then convert it to a pySpark dataframe
notebooks_pandas = pd.DataFrame(notebooks_as_dicts)
notebooks_df = spark.createDataFrame(notebooks_pandas)

# COMMAND ----------

display(notebooks_df)
