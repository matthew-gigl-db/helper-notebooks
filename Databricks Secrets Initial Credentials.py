# Databricks notebook source
# MAGIC %md
# MAGIC #### Set Databricks Widgets for Required Inputs

# COMMAND ----------

dbutils.widgets.text("db_pat", "", "Databricks Personal Access Token")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install the Databricks CLI

# COMMAND ----------

# install the Databricks CLI using a curl command and capture the response text
install_cmd_resp = !{"""curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"""}
install_cmd_resp

# COMMAND ----------

# parse the installation command response to know where the CLI was installed.  This may be '/root/bin/databricks' or '/usr/local/bin/databricks'.  
cli_path = install_cmd_resp[0].split("at ")[1].replace(".", "")
cli_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check the version of the Databricks CLI to ensure it installed correctly.  

# COMMAND ----------

version_cmd = f"{cli_path} -v"

!{version_cmd}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configure the Databricks CLI by passing in the full Workspace URL and the PAT
# MAGIC
# MAGIC The Workspace URL may be attained from the Databricks Spark Conf.  Note that its possible to return all of the values set in the Spark Conf as an array by running the following code:  
# MAGIC
# MAGIC > *spark_conf = spark.sparkContext.getConf()*  
# MAGIC > *spark_conf.getAll()*  
# MAGIC >   
# MAGIC
# MAGIC The Databricks Personal Access Token (PAT) will be retrieved using the Databricks Widgets Utility.  

# COMMAND ----------

# return the workspace url from the Databricks Spark Conf
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

# retrieve the user's Databricks Personal Access Token
db_pat = dbutils.widgets.get("db_pat")

print(f"""
  Workpace URL: {workspace_url}
""")

# COMMAND ----------

# configure the Databricks CLI on the cluster with the following command
configure_command = f"""echo '{db_pat}' | {cli_path} configure --host 'https://{workspace_url}'"""

!{configure_command}

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create the 'credentials' scope and populate it with the 'db_pat' secret

# COMMAND ----------

# list existing scopes 
existing_scopes_cmd = f"{cli_path} secrets list-scopes"

scopes = !{existing_scopes_cmd}
scopes

# COMMAND ----------

if 'credentials  DATABRICKS' in scopes: 
  print("The credentials scope is already set up.")
else: 
  create_scope_cmd = f"{cli_path} secrets create-scope credentials"
  !{create_scope_cmd}

# COMMAND ----------

# use the Secrets API to add or edit the PAT as a secret to the credentials scope.  
secrets_api_cmd = f"""curl --request POST "https://{workspace_url}/api/2.0/secrets/put" \
     --header "Authorization: Bearer {db_pat}" \
     --data '{{"scope": "credentials", "key": "databricks_pat", "string_value": "{db_pat}"}}' """

!{secrets_api_cmd}

# COMMAND ----------

# test the secret 
dbutils.secrets.get("credentials", "databricks_pat") == db_pat

# COMMAND ----------

# remove the widgets
dbutils.widgets.removeAll()
