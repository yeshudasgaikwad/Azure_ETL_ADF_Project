# Databricks notebook source
# MAGIC %md
# MAGIC **Connecting ADLS Using SAS**
# MAGIC

# COMMAND ----------

# DBTITLE 1,Useful Url
#databricksurl= https://adb-3960806779193035.15.azuredatabricks.net/

#create scope url= https://adb-3960806779193035.15.azuredatabricks.net#secrets/createScope

# COMMAND ----------

# DBTITLE 1,To see the secret scope
display(dbutils.secrets.listScopes())

# COMMAND ----------

# MAGIC %md
# MAGIC **secret scope is collection of secrets.**
# MAGIC

# COMMAND ----------

# DBTITLE 1,To list down the secrets which are stored in the scope

dbutils.secrets.list('service-scope')

# COMMAND ----------

# DBTITLE 1,Database Credentials Configuration
jdbc_url = dbutils.secrets.get("service-scope", "jdbc-url")
jdbc_user = dbutils.secrets.get("service-scope", "jdbc-user")
jdbc_pass = dbutils.secrets.get("service-scope", "jdbc-pass")

jdbc_props = {
    "user": jdbc_user,
    "password": jdbc_pass,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

# DBTITLE 1,Retrieving Top 5 Tables from Information Schema
df_test = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT TOP 5 * FROM INFORMATION_SCHEMA.TABLES) as t",
    properties=jdbc_props
)

df_test.show()


# COMMAND ----------

# DBTITLE 1,You can not see the password
dbutils.secrets.get(scope='service-scope',key='sp-client-secret')

# COMMAND ----------

# DBTITLE 1,Workaround to see the secret
# MAGIC %md
# MAGIC workaround to see the secret
# MAGIC x=dbutils.secrets.get(scope='service-scope',key='sp-client-secret')
# MAGIC
# MAGIC for i in x:
# MAGIC   print(i)

# COMMAND ----------

# DBTITLE 1,Storage Account Parameters
storage_account = "adlssalesprojectyeshudas"
service_credential = "eNk8Q~oo6m7_D4AcdmjOTYRHOXMd4KO4jha4-amc"
directory_id = "0ab56155-7f8f-4f59-a263-1dfb265b568d"
application_id = "d0a1f549-e398-483a-87f0-6789170ff5f5"
service_credential = dbutils.secrets.get(scope="service-scope",key="sp-client-secret")

# COMMAND ----------

# DBTITLE 1,Sql Server Parameters
#Server name = "salesprojectyeshudas.database.windows.net"
#Database_Name = "salesproject"
#User_Name = "salesuser"
# jdbc url= jdbc:sqlserver://salesprojectyeshudas.database.windows.net:1433;database=salesproject


# COMMAND ----------

# DBTITLE 1,Connecting to ADLS


spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Testing The connection

display(dbutils.fs.ls("abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target"))

# COMMAND ----------

# DBTITLE 1,Validation Functions
from pyspark.sql import functions as F

# 1. Primary Key
def validate_pk(df, col_name):
    good = df.dropDuplicates([col_name]).filter(F.col(col_name).isNotNull())
    bad = df.exceptAll(good)
    return good, bad

# 2. Foreign Key
def validate_fk(df, col_name, ref_df, ref_col):
    good = df.join(ref_df, df[col_name] == ref_df[ref_col], "inner")
    bad = df.join(ref_df, df[col_name] == ref_df[ref_col], "left_anti")
    return good, bad

# 3. Not Null + Zero + Negative check
def validate_notnull_zero_negative(df, col_name):
    good = df.filter((F.col(col_name).isNotNull()) & (F.col(col_name) > 0))
    bad = df.filter((F.col(col_name).isNull()) | (F.col(col_name) <= 0))
    return good, bad

# 4. Not Null only
def validate_notnull(df, col_name):
    good = df.filter(F.col(col_name).isNotNull())
    bad = df.filter(F.col(col_name).isNull())
    return good, bad

# 5. Date format check
def validate_date(df, col_name, fmt="yyyy-MM-dd"):
    good = df.filter(F.to_date(F.col(col_name), fmt).isNotNull())
    bad = df.filter(F.to_date(F.col(col_name), fmt).isNull())
    return good, bad


# COMMAND ----------

# DBTITLE 1,Validation Columns
validation_columns = {
    "ORDER_HEADER": {
        "PK": ["ORDER_NUMBER"],
        "NotNull": ["RETAILER_NAME"],
        "DATE": ["ORDER_DATE"]
    },
    "PRODUCT": {
        "PK": ["PRODUCT_NUMBER"],
        "DATE": ["INTRODUCTION_DATE", "DISCONTINUED_DATE"]
    },
    "ORDER_DETAILS": {
        "PK": ["ORDER_DETAIL_CODE"],
        "FK": ["ORDER_NUMBER"],
        "NotNullOrZeroOrNegative": ["QUANTITY", "UNIT_COST", "UNIT_PRICE", "UNIT_SALE_PRICE"]
    },
    "RETURNED_ITEM": {
        "PK": ["RETURN_CODE"],
        "FK": ["ORDER_DETAIL_CODE"]
    },
    "INVENTORY_LEVELS": {
        "NotNull": ["INVENTORY_YEAR"],
        "FK": ["PRODUCT_NUMBER"]
    }
}



# COMMAND ----------

# DBTITLE 1,Control file read
from pyspark.sql import SparkSession
import json

# Create Spark session
spark = SparkSession.builder.appName("RawToIngestion").getOrCreate()

# Path to control file in ADLS
control_file_path = "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/control/control_config.csv"

# Read control file
control_df = spark.read.option("header", True).csv(control_file_path)
control_df.show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Validation Functions Rules
from pyspark.sql.functions import col, when, lit
from pyspark.sql import DataFrame

def apply_validations(df: DataFrame, table_name: str, rules: dict):
    """
    Apply validation rules to dataframe and return (good_df, bad_df)
    with reject_reason column for bad records.
    """
    validations = rules.get(table_name, {})
    bad_df = df.withColumn("reject_reason", lit(None))

    # PK Validation → Cannot be null
    if "PK" in validations:
        for c in validations["PK"]:
            bad_df = bad_df.withColumn(
                "reject_reason",
                when(col(c).isNull(), lit(f"PK {c} is NULL")).otherwise(col("reject_reason"))
            )

    # FK Validation → Cannot be null
    if "FK" in validations:
        for c in validations["FK"]:
            bad_df = bad_df.withColumn(
                "reject_reason",
                when(col(c).isNull(), lit(f"FK {c} is NULL")).otherwise(col("reject_reason"))
            )

    # Not Null
    if "NotNull" in validations:
        for c in validations["NotNull"]:
            bad_df = bad_df.withColumn(
                "reject_reason",
                when(col(c).isNull(), lit(f"{c} is NULL")).otherwise(col("reject_reason"))
            )

    # Not Null, Zero or Negative
    if "NotNullOrZeroOrNegative" in validations:
        for c in validations["NotNullOrZeroOrNegative"]:
            bad_df = bad_df.withColumn(
                "reject_reason",
                when(col(c).isNull() | (col(c) <= 0), lit(f"{c} is NULL/<=0")).otherwise(col("reject_reason"))
            )

    # Date (must not be null)
    if "DATE" in validations:
        for c in validations["DATE"]:
            bad_df = bad_df.withColumn(
                "reject_reason",
                when(col(c).isNull(), lit(f"DATE {c} invalid/NULL")).otherwise(col("reject_reason"))
            )

    # Split into good and bad
    good_df = bad_df.filter(col("reject_reason").isNull()).drop("reject_reason")
    bad_df = bad_df.filter(col("reject_reason").isNotNull())

    return good_df, bad_df



# COMMAND ----------

# DBTITLE 1,Reading the files from adls forvalidation
df_order_header = spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/ORDER_HEADER*.csv",
    header=True,
    inferSchema=True
)


good_order_header_df, bad_order_header_df = apply_validations(df_order_header, "ORDER_HEADER", validation_columns)

# COMMAND ----------

df_product_new = spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/PRODUCT_20250914083107.csv",
    header=True,
    inferSchema=True,
    multiLine=True,     # in case any description column has line breaks
    escape="\""
)

# COMMAND ----------




df_order_details = spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/ORDER_DETAILS*.csv",
    header=True,
    inferSchema=True
)

df_returned_item = spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/RETURNED_ITEM*.csv",
    header=True,
    inferSchema=True
)

df_inventory_levels = spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/INVENTORY_LEVELS*.csv",
    header=True,
    inferSchema=True
)




# COMMAND ----------

good_product, bad_product = apply_validations(df_product_new, "PRODUCT", validation_columns)

print("PRODUCT - Good:", good_product.count(), " Bad:", bad_product.count())
bad_product.show(truncate=False)
display( bad_product.count())

# COMMAND ----------

good_order_details, bad_order_details = apply_validations(df_order_details, "ORDER_DETAILS", validation_columns)

print("ORDER_DETAILS - Good:", good_order_details.count(), " Bad:", bad_order_details.count())
bad_order_details.show(truncate=False)
display( bad_order_details.count())

# COMMAND ----------

good_returned_item, bad_returned_item = apply_validations(df_returned_item, "RETURNED_ITEM", validation_columns)

print("RETURNED_ITEM - Good:", good_returned_item.count(), " Bad:", bad_returned_item.count())
bad_returned_item.show(truncate=False)
display( bad_returned_item.count())

# COMMAND ----------

good_inventory_levels, bad_inventory_levels = apply_validations(df_inventory_levels, "INVENTORY_LEVELS", validation_columns)

print("INVENTORY_LEVELS - Good:", good_inventory_levels.count(), " Bad:", bad_inventory_levels.count())
bad_inventory_levels.show(truncate=False)
display( bad_inventory_levels.count())

# COMMAND ----------



bad_order_header_df.write.mode("overwrite").csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Rejected/ORDER_HEADER/",
    header=True
)

bad_returned_item.write.mode("overwrite").csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Rejected/RETURNED_ITEM/",
    header=True
)

bad_order_details.write.mode("overwrite").csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Rejected/ORDER_DETAILS/",
    header=True
)

bad_inventory_levels.write.mode("overwrite").csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Rejected/INVENTORY_LEVELS/",
    header=True
)

bad_product.write.mode("overwrite").csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Rejected/PRODUCT/",
    header=True
)

# COMMAND ----------



good_order_header_df.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/ORDER_HEADER/")

good_product.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/PRODUCT/")
    

good_order_details.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/ORDER_DETAILS/")   


good_returned_item.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/RETURNED_ITEM/")
    


good_inventory_levels.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/INVENTORY_LEVELS/")
    




# COMMAND ----------

# DBTITLE 1,load good data into the sql
good_order_header_df.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.ORDER_HEADER") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

good_product.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.PRODUCT") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

good_order_details.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.ORDER_DETAILS") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

good_returned_item.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.RETURNED_ITEM") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

good_inventory_levels.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.INVENTORY_LEVELS") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()


# COMMAND ----------

# DBTITLE 1,Loading the files with no validation to the sql
# The remainaing files with no validation required which directly has good df

df_retailer= spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/RETAILER*.csv",
    header=True,
    inferSchema=True
)

df_product_name_lookup= spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/PRODUCT_NAME_LOOKUP*.csv",
    header=True,
    inferSchema=True
)

df_country= spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/COUNTRY*.csv",
    header=True,
    inferSchema=True
)

df_branch= spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/BRANCH*.csv",
    header=True,
    inferSchema=True
)

df_order_method= spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/ORDER_METHOD*.csv",
    header=True,
    inferSchema=True
)

df_warehouse= spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/WAREHOUSE*.csv",
    header=True,
    inferSchema=True
)

df_return_reason= spark.read.csv(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Target/RETURN_REASON*.csv",
    header=True,
    inferSchema=True
)



# COMMAND ----------

# DBTITLE 1,Writing these files to integration level adls
df_retailer.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/RETAILER/")

df_product_name_lookup.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/PRODUCT_NAME_LOOKUP/")

df_country.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/COUNTRY/")

df_warehouse.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/WAREHOUSE/")

df_branch.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/BRANCH/")

df_return_reason.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/RETURN_REASON/")

df_order_method.write.mode("overwrite").parquet(
    "abfss://raw-container@adlssalesprojectyeshudas.dfs.core.windows.net/Integration/ORDER_METHOD/")







# COMMAND ----------

df_retailer.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.RETAILER") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

df_product_name_lookup.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.PRODUCT_NAME_LOOKUP") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

df_country.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.COUNTRY") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

df_warehouse.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.WAREHOUSE") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

df_order_method.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.ORDER_METHOD") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

df_return_reason.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.RETURN_REASON") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

df_branch.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url",jdbc_url) \
    .option("dbtable", "dbo.BRANCH") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

# COMMAND ----------

# DBTITLE 1,Data Post validations
display(df_order_method.count())

# COMMAND ----------

# DBTITLE 1,Order Method
staging_table = "stg.ORDER_METHOD"

df_order_method.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# DBTITLE 1,Staging Tables
staging_table="stg.WAREHOUSE"

df_warehouse.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

staging_table="stg.COUNTRY"

df_country.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

staging_table="stg.RETAILER"

df_retailer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

staging_table="stg.PRODUCT_NAME_LOOKUP"

df_product_name_lookup.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

staging_table="stg.RETURN_REASON"

df_return_reason.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# Example: ORDER_DETAILS
staging_table = "stg.ORDER_DETAILS"

good_order_details.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()



# COMMAND ----------

staging_table="stg.ORDER_HEADER"

good_order_header_df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

staging_table="stg.PRODUCT"

good_product.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()


# COMMAND ----------

staging_table="stg.INVENTORY_LEVELS"

good_inventory_levels.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

staging_table="stg.RETURNED_ITEM"

good_returned_item.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .mode("overwrite") \
    .save()