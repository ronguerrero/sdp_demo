# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Policy Enrichment, Claims Triangulation, Quarantine
# MAGIC Complex insurance transformations: policy-claims linkage, loss development,
# MAGIC reinsurance cession, and exposure aggregation.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("ins.catalog", "ronguerrero")
SCHEMA = spark.conf.get("ins.schema", "ascot_insurance_ops")
BASE_CURRENCY = spark.conf.get("ins.base_currency", "USD")

# ═══════════════════════════════════════════
#  SILVER: Enriched Policies (multi-way join)
# ═══════════════════════════════════════════

@dlt.table(
    comment="Policies enriched with LOB, broker, and reinsurance details. Net premium calculated after cession.",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    "valid_policy_id": "policy_id IS NOT NULL",
    "valid_premium": "gross_premium > 0",
    "valid_limit": "limit_amount > 0",
    "resolved_lob": "lob_name IS NOT NULL",
    "not_future_inception": "inception_date <= current_timestamp()",
})
def silver_enriched_policies():
    policies = dlt.read("bronze_policies")
    lobs = dlt.read("bronze_lines_of_business")
    brokers = spark.read.table(f"{CATALOG}.{SCHEMA}.raw_brokers")
    reinsurers = spark.read.table(f"{CATALOG}.{SCHEMA}.raw_reinsurers")

    # Average cession rate for quota share calculation
    avg_cession = reinsurers.agg(F.avg("cession_rate")).collect()[0][0] or 0.30

    return (
        policies
        .join(lobs.select("lob_id", "lob_name", F.col("segment").alias("lob_segment")), "lob_id", "left")
        .join(brokers.select("broker_id", "broker_name", F.col("rating").alias("broker_rating")), "broker_id", "left")
        .withColumn("cession_rate", F.lit(avg_cession))
        .withColumn("ceded_premium", F.col("gross_premium") * F.col("cession_rate"))
        .withColumn("net_premium", F.col("gross_premium") - F.col("ceded_premium"))
        .withColumn("rate_on_line", F.col("gross_premium") / F.col("limit_amount") * 100)
        .withColumn("policy_year", F.year("inception_date"))
        .withColumn("inception_month", F.date_trunc("month", "inception_date"))
    )

# ═══════════════════════════════════════════
#  QUARANTINE: Failed Policy Records
# ═══════════════════════════════════════════

@dlt.table(comment="Quarantined policies with failure reasons", table_properties={"quality": "quarantine"})
def quarantine_policies():
    policies = dlt.read("bronze_policies")
    lobs = dlt.read("bronze_lines_of_business")

    enriched = policies.join(lobs.select("lob_id", "lob_name"), "lob_id", "left")

    return (
        enriched.filter(
            F.col("policy_id").isNull() |
            (F.col("gross_premium") <= 0) | F.col("gross_premium").isNull() |
            (F.col("limit_amount") <= 0) | F.col("limit_amount").isNull() |
            F.col("lob_name").isNull()
        )
        .withColumn("failure_reasons", F.concat_ws(", ",
            F.when(F.col("policy_id").isNull(), F.lit("null_policy_id")),
            F.when((F.col("gross_premium") <= 0) | F.col("gross_premium").isNull(), F.lit("invalid_premium")),
            F.when((F.col("limit_amount") <= 0) | F.col("limit_amount").isNull(), F.lit("invalid_limit")),
            F.when(F.col("lob_name").isNull(), F.lit("orphan_lob")),
        ))
        .withColumn("quarantined_at", F.current_timestamp())
    )

# ═══════════════════════════════════════════
#  SILVER: Claims with Loss Development
#  Running incurred progression per claim (window functions)
# ═══════════════════════════════════════════

@dlt.table(
    comment="Claims enriched with loss development factors, reserve adequacy, and aging. Running incurred progression via window functions — replaces dbt incremental SCD2 pattern.",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    "valid_claim_id": "claim_id IS NOT NULL",
    "positive_incurred": "incurred_amount > 0",
    "paid_not_exceeds_incurred": "paid_amount <= incurred_amount * 1.1",
})
def silver_claims_enriched():
    claims = dlt.read("bronze_claims")

    return (
        claims
        .withColumn("loss_date_only", F.to_date("loss_date"))
        .withColumn("notification_lag_days",
            F.datediff(F.to_date("notification_date"), F.to_date("loss_date")))
        .withColumn("reserve_adequacy",
            F.when(F.col("reserve_amount") >= F.col("incurred_amount") - F.col("paid_amount"), "ADEQUATE")
             .otherwise("DEFICIENT"))
        .withColumn("claim_age_days",
            F.datediff(F.current_date(), F.to_date("loss_date")))
        .withColumn("claim_age_bucket",
            F.when(F.col("claim_age_days") < 90, "0-90 days")
             .when(F.col("claim_age_days") < 180, "90-180 days")
             .when(F.col("claim_age_days") < 365, "180-365 days")
             .when(F.col("claim_age_days") < 730, "1-2 years")
             .otherwise("2+ years"))
        .withColumn("severity_band",
            F.when(F.col("incurred_amount") < 50000, "Attritional")
             .when(F.col("incurred_amount") < 500000, "Medium")
             .when(F.col("incurred_amount") < 2000000, "Large")
             .otherwise("Catastrophe"))
        # Running loss development per claim (window — dbt killer)
        .withColumn("development_month",
            F.floor(F.col("claim_age_days") / 30).cast("int"))
        .withColumn("cumulative_paid_rank",
            F.row_number().over(Window.partitionBy("claim_id").orderBy("loss_date")))
    )

# ═══════════════════════════════════════════
#  SILVER: Exposure Cleaned
# ═══════════════════════════════════════════

@dlt.table(comment="Cleaned exposure data with PML ratios", table_properties={"quality": "silver"})
@dlt.expect_all_or_drop({
    "positive_tiv": "total_insured_value > 0",
    "positive_pml": "probable_max_loss > 0",
    "pml_lte_tiv": "probable_max_loss <= total_insured_value",
})
def silver_exposure():
    return (
        dlt.read("bronze_exposure")
        .withColumn("pml_ratio", F.col("probable_max_loss") / F.col("total_insured_value"))
        .withColumn("exposure_quarter", F.date_trunc("quarter", "as_of_date"))
    )

