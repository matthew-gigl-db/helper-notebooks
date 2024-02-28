# databricks-setup
Notebooks To Help Users or Admins with Various Databricks Setup Steps

Current notebooks available: 

For all users: 
  * **Databricks Secrets Initial Credentials** helps a user create a Databricks Secret Scope called "credentials" and stores or updates the user's Databricks Personal Access Token as the secret "databricks_pat" for later use with the Databricks CLI, Databricks SDKs or the Databricks REST APIs directly.  

For Unity Catalog admins:
  * **Unity Catalog's System Catalog Schema Set Up** helps an admin with account or metastore management permissons the ability to monitor and enable new schemas that might become available the "system" catalog.  The "system" catalog has several schemas already that help with monitoring all workspaces in an account including for auditing user commands and access, or for monitoring FinOps.  
