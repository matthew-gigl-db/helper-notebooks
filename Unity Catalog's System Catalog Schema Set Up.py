# Databricks notebook source
# DBTITLE 1,Upgrading Databricks SDK Using Pip Command
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Restart Python to Use Upgraded Databricks SDK
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Configure the SDK's Workspace Client
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,List Account Metastores
metastores = w.metastores.list()
metastores = [m.as_dict() for m in metastores]

# COMMAND ----------

# DBTITLE 1,Display Metastores
metastores

# COMMAND ----------

# DBTITLE 1,For Each Metastore, List the System Schemas Available
system_schemas_all = []
for m in metastores:
    system_schemas = w.system_schemas.list(m["metastore_id"])
    system_schemas = [s.as_dict() for s in system_schemas]
    for s in system_schemas:
        s["metastore_id"] = m["metastore_id"]
    system_schemas_all.extend(system_schemas)

# COMMAND ----------

# DBTITLE 1,Display the State of Each System Schema
import pandas as pd

system_schemas_df = pd.DataFrame(system_schemas_all)
system_schemas_df = spark.createDataFrame(system_schemas_df)
display(system_schemas_df)

# COMMAND ----------

# DBTITLE 1,Enable Each Available Schema
for s in system_schemas_all:
    if s["state"] == "AVAILABLE":
        w.system_schemas.enable(metastore_id=s["metastore_id"], schema_name=s["schema"])

# COMMAND ----------

# DBTITLE 1,Review Results
system_schemas_all = []
for m in metastores:
    system_schemas = w.system_schemas.list(m["metastore_id"])
    system_schemas = [s.as_dict() for s in system_schemas]
    for s in system_schemas:
        s["metastore_id"] = m["metastore_id"]
    system_schemas_all.extend(system_schemas)

system_schemas_df = pd.DataFrame(system_schemas_all)
system_schemas_df = spark.createDataFrame(system_schemas_df)
display(system_schemas_df)

# COMMAND ----------

# DBTITLE 1,Bonus:  Uncomment and Update Disable List to Turn Remove a System Catalog Schema
# disable_list = ["query", "lakeflow"]

# for d in disable_list:
#     w.system_schemas.disable(metastore_id=m["metastore_id"], schema_name=d)
