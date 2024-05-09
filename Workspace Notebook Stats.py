# Databricks notebook source
dbutils.widgets.text("path", "/", "Workspace Path")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
wc = WorkspaceClient()

# COMMAND ----------

notebooks = wc.workspace.list(path = dbutils.widgets.get("path"), recursive=True)

# COMMAND ----------

type(notebooks)

# COMMAND ----------

notebooks_list = list(notebooks)

# COMMAND ----------

notebooks_list

# COMMAND ----------

type(notebooks_list)

# COMMAND ----------

# Convert notebooks list to a DataFrame
notebooks_df = spark.createDataFrame(notebooks_list)

# Display the DataFrame
display(notebooks_df)
