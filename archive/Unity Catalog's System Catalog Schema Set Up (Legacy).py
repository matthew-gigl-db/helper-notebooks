# Databricks notebook source
# MAGIC %md
# MAGIC # United Catalog's System Catalog Schema Set Up
# MAGIC
# MAGIC There are several schemas that are available for monitoring your Databricks account, however these schemas need to be enabled by a user with account privlidges such as an admin.  
# MAGIC
# MAGIC Prerequites:  
# MAGIC
# MAGIC * At least one workspace that is set up with Unity Catalog for the account and the **Workspace ID** of one of those UC enabled workspaces.  This notebook should be run on one of those workspaces.  
# MAGIC * The user running this notebook must have a Databricks Personal Access Token (PAT) saved as a Databricks Secret for the particular workspace used.  The Databricks secret scope and the secret name for the PAT are inputted using Databricks text widgets.  
# MAGIC
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Set Up

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Databricks Widgets for Required Inputs

# COMMAND ----------

# DBTITLE 1,Widgets Set Up
# Add a widget for the Databricks Secret Scope used for storing the user's Databricks Personal Access Token  
dbutils.widgets.text("pat_secret_scope", "credentials", "DB Secret Scope for PAT")

# Add a widget for the Databricks Secret representing the Databricks Personal Access Token  
dbutils.widgets.text("pat_secret", "databricks_pat", "DB Secret for PAT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks CLI Set Up and Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install the Databricks CLI

# COMMAND ----------

# DBTITLE 1,Install the CLI
# install the Databricks CLI using a curl command and capture the response text
install_cmd_resp = !{"curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"}
install_cmd_resp

# COMMAND ----------

# DBTITLE 1,Capture the CLI Path
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

# DBTITLE 1,Check CLI Version and Installation
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
# MAGIC The Databricks Personal Access Token (PAT) will be retrieved using the Databricks Secrets Utility.  

# COMMAND ----------

# DBTITLE 1,Retrieve the Workspace URL and Personal Access Token
# return the workspace url from the Databricks Spark Conf
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

# retrieve the user's Databricks Personal Access Token
db_pat = dbutils.secrets.get(
  scope = dbutils.widgets.get("pat_secret_scope")
  ,key = dbutils.widgets.get("pat_secret")
)

print(f"""
  Workpace URL: {workspace_url}
  Databricks PAT: {db_pat}
""")

# COMMAND ----------

# DBTITLE 1,Configure the CLI
# configure the Databricks CLI on the cluster with the following command
configure_command = f"""echo '{db_pat}' | {cli_path} configure --host 'https://{workspace_url}'"""

!{configure_command}

# COMMAND ----------

# DBTITLE 1,Check Configuration
check_cli_cmd = f"{cli_path} current-user me"
!{check_cli_cmd}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog System Catalog Set Up

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Return the UC metastore summary to get the metastore ids.  

# COMMAND ----------

# DBTITLE 1,Metastore Summary

metastore_summary_cmd = f"{cli_path} metastores summary"

metastore_summary = !{metastore_summary_cmd } 
metastore_summary

# COMMAND ----------

# DBTITLE 1,Convert to JSON
# Convert metastore_summary string to JSON string
metastore_json_string = ''.join(metastore_summary)
metastore_json_string

# COMMAND ----------

# DBTITLE 1,Convert JSON to Spark Dataframe
# Create DataFrame with the json string
df = spark.read.json(spark.sparkContext.parallelize([metastore_json_string]))
display(df)

# COMMAND ----------

# DBTITLE 1,Return the Metastore IDs
metastore_ids = df.select("metastore_id").rdd.flatMap(lambda x: x).collect()
metastore_ids

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check the status of the system catalog schemas for the metastore ids

# COMMAND ----------

# DBTITLE 1,Get the schema statuses for each metastore
import requests

system_schema_status = []
for metastore in metastore_ids:
  url = f"""https://{workspace_url}/api/2.0/unity-catalog/metastores/{metastore}/systemschemas"""
  headers = {"Authorization": f"Bearer {db_pat}"}
  response = requests.get(url = url, headers = headers)
  status = response.json()
  status["metastore_id"] = metastore
  system_schema_status.append(status)

system_schema_status

# COMMAND ----------

from pyspark.sql.functions import col, explode

schema_status_df = (spark.createDataFrame(
    spark.sparkContext.parallelize(system_schema_status), ["metastore_id","schemas"]
  )
  .withColumn("schemas", explode(col("schemas")))
  .withColumn("schema", col("schemas.schema"))
  .withColumn("state", col("schemas.state"))
  .select("metastore_id", "schema", "state")
)

display(schema_status_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable Available System Catalog Schemas

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define a Spark UDF to Enable Any Schemas in each Metastore with an "AVAILABLE" State

# COMMAND ----------

# DBTITLE 1,Define enablement spark UDF
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def enable_system_schema(metastore_id: str, schema: str, state: str, databricks_pat: str = db_pat, workspace_url: str = workspace_url) -> int:
  if state == "AVAILABLE":
    url = f"https://{workspace_url}/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas/{schema}"
    headers = {"Authorization": f"Bearer {databricks_pat}"}
    response = requests.put(url, headers=headers)
    return response.status_code

# COMMAND ----------

# DBTITLE 1,Execute the UDF Against the Dataframe
enable_df = (schema_status_df
  .withColumn("enablement_status_code",
    enable_system_schema(
      metastore_id=col("metastore_id")
      ,schema=col("schema")
      ,state=col("state")
    ))
)

display(enable_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Status codes of 200 indicate that the schema was successfully enabled.  Null values result for schemas that the API was not called on, such as for schemas that are already enabled ("ENABLE_COMPLETED") or currently unavailable.  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify a new System Catalog Schema is available in Unity Catalog

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from system.access.audit limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Disable System Schema Function

# COMMAND ----------

# DBTITLE 1,Define function to disable a system schema
def disable_system_schema(metastore_id: str, schema: str, databricks_pat: str = db_pat, workspace_url: str = workspace_url) -> int:
  url = f"https://{workspace_url}/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas/{schema}"
  headers = {"Authorization": f"Bearer {databricks_pat}"}
  response = requests.delete(url, headers=headers)
  return response.status_code

# COMMAND ----------

# DBTITLE 1,Not run: disable schema example
# # not run 
# disable_system_schema(metastore_id="fe90da00-0714-4d15-b3ed-60de3697184a", schema="storage")
