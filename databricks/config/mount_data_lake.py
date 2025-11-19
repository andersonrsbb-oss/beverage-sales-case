# Databricks notebook source
#ADLS
storage_account_name = "adlsdataengineeringprd"

#APP REGISTRATION
client_id = dbutils.secrets.get(scope="adls-secrets", key="client-id")
tenant_id = dbutils.secrets.get(scope="adls-secrets", key="tenant-id")
client_secret = dbutils.secrets.get(scope="adls-secrets", key="client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": f"{client_id}",
"fs.azure.account.oauth2.client.secret": f"{client_secret}",
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# dbtuils.fs.mount( source = "", mount_point = 'mnt/caminho...', extra_configs = {})


# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls('beverage-datalake')

# COMMAND ----------

mount_adls('landing-zone')

# COMMAND ----------

mount_adls('bronze')

# COMMAND ----------

mount_adls('silver')

# COMMAND ----------

mount_adls('gold')