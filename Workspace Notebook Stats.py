# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieving Workspace Notebook Details Using the Python SDK
# MAGIC ***

# COMMAND ----------

# DBTITLE 1,Set Workspace Path to Review
dbutils.widgets.text("path", "/", "Workspace Path")

# COMMAND ----------

# DBTITLE 1,Initialzie the Workspace Client
from databricks.sdk import WorkspaceClient
wc = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,List Notebooks in Workspace
notebooks = wc.workspace.list(path = dbutils.widgets.get("path"), recursive=True)

# COMMAND ----------

# DBTITLE 1,Note That The Result is a Generator Type
type(notebooks)

# COMMAND ----------

notebooks_sdf = spark.createDataFrame(notebooks).take(10)

# COMMAND ----------



# COMMAND ----------

notebooks_list = list(notebooks)

# COMMAND ----------

notebooks_list

# COMMAND ----------

# Convert the notebooks generator into a pandas DataFrame
import pandas as pd

notebooks_pd = pd.DataFrame(notebooks_list)

# COMMAND ----------

notebooks_pd

# COMMAND ----------

# Convert pandas DataFrame to Spark DataFrame
notebooks_df = spark.createDataFrame(notebooks_pd)
display(notebooks_df)

# COMMAND ----------

# Convert notebooks list to a DataFrame
notebooks_df = spark.createDataFrame(notebooks_list)

# Display the DataFrame
display(notebooks_df)
