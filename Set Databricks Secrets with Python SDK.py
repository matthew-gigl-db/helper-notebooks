# Databricks notebook source
# DBTITLE 1,Upgrading the Databricks SDK with Pip
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0]
scope_name = user_name.split(sep="@")[0].replace(".", "-")
scope_name

# COMMAND ----------

# DBTITLE 1,Setup Input Parameters
dbutils.widgets.text("secret_scope", scope_name, "Databricks Secret Scope")
dbutils.widgets.text("secret_key", "", "Databricks Secret Key")
dbutils.widgets.text("secret_key_value", "", "Secret Key Value")

# COMMAND ----------

# DBTITLE 1,Retrieve Input Parameters
secret_scope = dbutils.widgets.get("secret_scope")
secret_scope_key = dbutils.widgets.get("secret_key")
secret_key_value = dbutils.widgets.get("secret_key_value")

# COMMAND ----------

# DBTITLE 1,Databricks Workspace Client Initialization
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Listing Secret Scopes as Dictionaries in Python
scopes = w.secrets.list_scopes()
scopes = [scope.as_dict() for scope in scopes]

# COMMAND ----------

# DBTITLE 1,Check for Databricks Scope Name in Scopes
scope_exists = any(scope['name'] == secret_scope for scope in scopes)
display(scope_exists)

# COMMAND ----------

# DBTITLE 1,Create Scope If Not Exists
if scope_exists == True:
  print(f"{secret_scope} secret scope already exists")
else:
  w.secrets.create_scope(scope=secret_scope)

# COMMAND ----------

# DBTITLE 1,Set the Databricks Secret Key with a New Value
if secret_key_value not in [None, '']:
  w.secrets.put_secret(scope=secret_scope, key=secret_scope_key, string_value=secret_key_value)

# COMMAND ----------

# DBTITLE 1,Retrieving Encryption Key from Databricks Secrets
dbutils.secrets.get(scope=secret_scope, key=secret_scope_key) == secret_key_value

# COMMAND ----------

# DBTITLE 1,Clean Up
dbutils.widgets.removeAll()
