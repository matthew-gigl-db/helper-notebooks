# Databricks notebook source
from databricks.sdk import WorkspaceClient
wc = WorkspaceClient()

# COMMAND ----------

notebooks = wc.workspace.list(path = "/", recursive=True)

# COMMAND ----------

type(notebooks)

# COMMAND ----------

notebooks_list = list(notebooks)
notebooks_list[0]

# COMMAND ----------

# Convert notebooks list to a DataFrame
notebooks_df = spark.createDataFrame(notebooks_list)

# Display the DataFrame
display(notebooks_df)
