# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Insurance Data Ingestion
# MAGIC Inline expectations track quality without blocking ingestion.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("ins.catalog", "ronguerrero")
SCHEMA = spark.conf.get("ins.schema", "ascot_insurance_ops")

@dlt.table(comment="Raw policy submissions and bindings", table_properties={"quality": "bronze"})
@dlt.expect("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect("valid_premium", "gross_premium IS NOT NULL AND gross_premium > 0")
@dlt.expect("valid_limit", "limit_amount IS NOT NULL AND limit_amount > 0")
@dlt.expect("valid_lob", "lob_id IS NOT NULL")
def bronze_policies():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.raw_policies")

@dlt.table(comment="Raw claims notifications and movements", table_properties={"quality": "bronze"})
@dlt.expect("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect("valid_incurred", "incurred_amount IS NOT NULL")
@dlt.expect("valid_policy_ref", "policy_id IS NOT NULL")
def bronze_claims():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.raw_claims")

@dlt.table(comment="Raw catastrophe exposure accumulations", table_properties={"quality": "bronze"})
@dlt.expect("valid_exposure", "total_insured_value IS NOT NULL")
@dlt.expect("valid_territory", "territory IS NOT NULL")
def bronze_exposure():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.raw_exposure_accumulations")

@dlt.table(comment="Lines of business reference", table_properties={"quality": "bronze"})
@dlt.expect("valid_lob_id", "lob_id IS NOT NULL")
def bronze_lines_of_business():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.raw_lines_of_business")

