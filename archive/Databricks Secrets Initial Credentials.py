# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Secrets Initial Credentials Set Up
# MAGIC
# MAGIC > **Special Note: This version of credentials set up only works with Databricks Classic Compute Runtimes.  Please see the follow notebook for setting up credentials with Serverless Compute and a more modern method with the Databricks Python SDK:**  
# MAGIC > [https://github.com/matthew-gigl-db/helper-notebooks](https://github.com/matthew-gigl-db/helper-notebooks)  
# MAGIC   
# MAGIC
# MAGIC The purpose of this notebook is to programatically allow a Databricks user to establish a personal Databricks Secret Scope for storing the user's credentials starting with the user's Databricks Personal Access Token, which is required for using the Databricks APIs or to configure the Databricks CLI.  
# MAGIC
# MAGIC This notebook is intended to be run interactively by the user.  A Databricks Text widget allows the user to input their Databricks Personal Access token, and then after a successful run of the commands, the widget is removed to prevent any accidental storing of the PAT in the Databricks workspace.  
# MAGIC
# MAGIC The notebook performs the following actions:  
# MAGIC
# MAGIC 1. Registers the **db_pat** as a Databricks Text Widget.  
# MAGIC 1. Installs the lastest version of the Databricks CLI on the cluster and capture where that installation was saved for use in future shell commands.  
# MAGIC 1. Checks that the Databricks CLI was installed correctly.  
# MAGIC 1. Configures the Databricks CLI using the workspace URL (captured from the Databricks Spark Conf), and the user inputted Personal Access Token.  
# MAGIC 1. Using the Databricks CLI, list the existing Secret Scopes available in the workspace.  If scope based on the user id doesn't exist, use the CLI to register that scope new.  
# MAGIC 1. Using the Databricks Secrets API, create or update the "databricks_pat" secret in the user's "credentials" scope and verify that its set correctly.  
# MAGIC 1. Remove the **db_pat** widget from the Notebook to prevent any accidental storage.  
# MAGIC
# MAGIC Run this notebook as often as the Databricks PAT needs to be updated.  Once the initial user's "credentials" scope has been created another notebook may be used to add or update additional Databricks Secrets or Secret Scopes.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Databricks Widgets for Required Inputs

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("db_pat", "", "Databricks Personal Access Token")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install the Databricks CLI

# COMMAND ----------

# DBTITLE 1,Install the CLI
# install the Databricks CLI using a curl command and capture the response text
install_cmd_resp = !{"""curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"""}
install_cmd_resp

# COMMAND ----------

# DBTITLE 1,Record the ClI installation path
# parse the installation command response to know where the CLI was installed.  This may be '/root/bin/databricks' or '/usr/local/bin/databricks'.  
if install_cmd_resp[0][0:4] == "Inst":
  cli_path = install_cmd_resp[0].split("at ")[1].replace(".", "")
else:
  cli_path = install_cmd_resp[0].split(" ")[2]
cli_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check the version of the Databricks CLI to ensure it installed correctly.  

# COMMAND ----------

# DBTITLE 1,Verify the version
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

# DBTITLE 1,Retrieve the workspace url and DB PAT
# return the workspace url from the Databricks Spark Conf
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

# retrieve the user's Databricks Personal Access Token
db_pat = dbutils.widgets.get("db_pat")

print(f"""
  Workpace URL: {workspace_url}
""")

# COMMAND ----------

# DBTITLE 1,Configure the CLI
# configure the Databricks CLI on the cluster with the following command
configure_command = f"""echo '{db_pat}' | {cli_path} configure --host 'https://{workspace_url}'"""

!{configure_command}

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create the user's 'credentials' scope based on the user name and populate it with the 'databricks_pat' secret

# COMMAND ----------

# DBTITLE 1,List existing DB Secret Scopes
# list existing scopes 
existing_scopes_cmd = f"{cli_path} secrets list-scopes"

scopes = !{existing_scopes_cmd}
scopes

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0]
scope_name = user_name.split(sep="@")[0].replace(".", "-")
scope_name

# COMMAND ----------

# DBTITLE 1,Check if the "credentials" scope is already available.
if f"{scope_name}  DATABRICKS" in scopes: 
  print("The credentials scope is already set up.")
else: 
  create_scope_cmd = f"{cli_path} secrets create-scope {scope_name}"
  !{create_scope_cmd}

# COMMAND ----------

# DBTITLE 1,Set the "databricks_pat" secret
# use the Secrets API to add or edit the PAT as a secret to the credentials scope.  
secrets_api_cmd = f"""curl --request POST "https://{workspace_url}/api/2.0/secrets/put" \
     --header "Authorization: Bearer {db_pat}" \
     --data '{{"scope": "{scope_name}", "key": "databricks_pat", "string_value": "{db_pat}"}}' """

!{secrets_api_cmd}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test that the secret was set correctly

# COMMAND ----------

# DBTITLE 1,Test the new secret is correct
# test the secret 
dbutils.secrets.get(scope_name, "databricks_pat") == db_pat

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up the notebook.  

# COMMAND ----------

# DBTITLE 1,Remove the widget
# remove the widgets
dbutils.widgets.removeAll()
