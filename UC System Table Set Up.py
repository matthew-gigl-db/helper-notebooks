# Databricks notebook source
# MAGIC %md
# MAGIC # United Catalog System Catalog Schema Set Up
# MAGIC
# MAGIC There are several schemas that are available for monitoring your Databricks account, however these schemas need to be enabled by a user with account privlidges such as an admin.  
# MAGIC
# MAGIC Prerequites:  
# MAGIC
# MAGIC * At least one workspace that is set up with Unity Catalog for the account and the **Workspace ID** of one of those UC enabled workspaces.  This notebook should be run on one of those workspaces.  
# MAGIC * The user running this notebook must have a Databricks Personal Access Token (PAT) saved as a Databricks Secret for the particular workspace used.  The Databricks secret scope and the secret name for the PAT are inputted using Databricks text widgets.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Databricks Widgets for Required Inputs

# COMMAND ----------

# Add a widget for workspace id
dbutils.widgets.text("pat_secret_scope", "", "DB Secret Scope for PAT")

# Add a widget for DB PAT
dbutils.widgets.text("pat_secret", "", "DB Secret for PAT")

# COMMAND ----------

workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
workspace_id

# COMMAND ----------

# # return the workspace url from the Databricks Spark Conf, note that you can return all of the Spark configuragion items in an array with the following:  
# spark_conf = spark.sparkContext.getConf()
# spark_conf.getAll()
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
print(workspace_url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install the Databricks CLI

# COMMAND ----------

bash_command = f"curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"

!{bash_command}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check the version of the Databricks CLI to ensure it installed correctly.  

# COMMAND ----------

!{"databricks -v"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configure the Databricks CLI by passing in the full workspace URL and the PAT

# COMMAND ----------

workspace_url = f"""https://{dbutils.widgets.get("workspace_id")}.cloud.databricks.com"""

configure_command = f"""echo '{dbutils.widgets.get("db_pat")}' | databricks configure --host '{workspace_url}'"""

!{configure_command}

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Return the UC metastore summary to get the metastore ids.  

# COMMAND ----------

metastore_summary = !{'databricks metastores summary'} 
metastore_summary

# COMMAND ----------

# Convert metastore_summary string to JSON string
metastore_json_string = ''.join(metastore_summary)
metastore_json_string

# COMMAND ----------

# Create DataFrame with the json string
df = spark.read.json(spark.sparkContext.parallelize([metastore_json_string]))
display(df)

# COMMAND ----------

metastore_ids = df.select("metastore_id").rdd.flatMap(lambda x: x).collect()
metastore_ids

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check the status of the systemschemas for the metastore ids

# COMMAND ----------

system_schema_status = []
for metastore in metastore_ids:
  systemschemas_command = f"""curl -X GET -H "Authorization: Bearer {dbutils.widgets.get("db_pat")}" "{workspace_url}/api/2.0/unity-catalog/metastores/{metastore}/systemschemas" """
  status = !{systemschemas_command}
  status.append(metastore)
  system_schema_status += status

# turn into json
# system_schema_status = "".join(system_schema_status)
system_schema_status

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

schema = StructType([
    StructField("col1", StringType(), True)
    ,StructField("col2", StringType(), True)
    ,StructField("col3", StringType(), True)
    ,StructField("col4", StringType(), True)
    ,StructField("col5", StringType(), True)
    ,StructField("col6", StringType(), True)
    ,StructField("schema_status", StringType(), True)
    ,StructField("metastore_id", StringType(), True)

])

# COMMAND ----------

sample_schema_status_json = """
{"schemas":[{"schema":"storage","state":"ENABLE_COMPLETED"},{"schema":"access","state":"ENABLE_COMPLETED"},{"schema":"billing","state":"ENABLE_COMPLETED"},{"schema":"compute","state":"ENABLE_COMPLETED"},{"schema":"marketplace","state":"ENABLE_COMPLETED"},{"schema":"operational_data","state":"UNAVAILABLE"},{"schema":"lineage","state":"ENABLE_COMPLETED"},{"schema":"information_schema","state":"ENABLE_COMPLETED"}]}
"""

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, schema_of_json

schema_status_df = (spark
  .createDataFrame(spark.sparkContext.parallelize([system_schema_status]), schema = schema)
  .withColumn("schema_status", from_json(col("schema_status"), schema=schema_of_json(sample_schema_status_json)))
  .withColumn("schemas", explode(col("schema_status.schemas")))
  .withColumn("schema", col("schemas.schema"))
  .withColumn("state", col("schemas.state"))
  .select("metastore_id", "schema", "state")

)

display(schema_status_df)

# COMMAND ----------

available_to_enable = schema_status_df.filter(col("state") == "AVAILABLE").select("schema").rdd.flatMap(lambda x: x).collect()

available_to_enable

# COMMAND ----------

for system_schema in available_to_enable:
  enablement_command = f"""curl -v -X PUT -H "Authorization: Bearer {dbutils.widgets.get("db_pat")}" "{workspace_url}/api/2.0/unity-catalog/metastores/{metastore}/systemschemas/{system_schema}" """
  {enablement_command}

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from system.access.audit
