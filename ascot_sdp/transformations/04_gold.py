# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Underwriting Analytics, Loss Ratios, Compliance
# MAGIC Business-critical insurance calculations with parameterized thresholds.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("ins.catalog", "ronguerrero")
SCHEMA = spark.conf.get("ins.schema", "ascot_insurance_ops")
MAX_SINGLE_RISK = float(spark.conf.get("ins.max_single_risk_pct", "10.0"))
MAX_PERIL_CONC = float(spark.conf.get("ins.max_peril_concentration_pct", "25.0"))
MIN_LOSS_RATIO = float(spark.conf.get("ins.min_loss_ratio_threshold", "0.75"))
MAX_AGG_LIMIT = float(spark.conf.get("ins.max_aggregate_limit_usd", "500000000"))

# ═══════════════════════════════════════════
#  GOLD: Underwriting Summary
#  GWP, NWP, loss ratios, combined ratios by LOB
# ═══════════════════════════════════════════

@dlt.table(
    comment="Underwriting performance by LOB and policy year: GWP, NWP, earned premium, incurred losses, loss ratio, combined ratio.",
    table_properties={"quality": "gold"}
)
def gold_underwriting_summary():
    policies = dlt.read("silver_enriched_policies")
    claims = dlt.read("silver_claims_enriched")

    # Premium by LOB and year
    premium = (
        policies.filter(F.col("status") == "BOUND")
        .groupBy("lob_id", "lob_name", "lob_segment", "policy_year")
        .agg(
            F.sum("gross_premium").alias("gwp"),
            F.sum("net_premium").alias("nwp"),
            F.sum("ceded_premium").alias("ceded_premium"),
            F.count("*").alias("policy_count"),
            F.avg("rate_on_line").alias("avg_rate_on_line"),
            F.sum("limit_amount").alias("total_limit"),
        )
    )

    # Losses by claim year (using loss_date year, joining to policy peril)
    losses = (
        claims
        .withColumn("loss_year", F.year("loss_date"))
        .groupBy("peril", "loss_year")
        .agg(
            F.sum("incurred_amount").alias("incurred_losses"),
            F.sum("paid_amount").alias("paid_losses"),
            F.sum("reserve_amount").alias("outstanding_reserves"),
            F.count("*").alias("claim_count"),
            F.avg("notification_lag_days").alias("avg_notification_lag"),
        )
    )

    # Join premium with losses (approximate — by year)
    combined = premium.join(
        losses.withColumnRenamed("loss_year", "policy_year"),
        "policy_year", "left"
    )

    return combined \
        .withColumn("loss_ratio", F.col("incurred_losses") / F.col("nwp")) \
        .withColumn("expense_ratio", F.lit(0.32)) \
        .withColumn("combined_ratio", F.col("loss_ratio") + F.lit(0.32)) \
        .withColumn("underwriting_profit",
            F.col("nwp") - F.col("incurred_losses") - (F.col("nwp") * 0.32))

# ═══════════════════════════════════════════
#  GOLD: Compliance Checks
#  Parameterized from pipeline config
# ═══════════════════════════════════════════

@dlt.table(
    comment=f"Automated compliance: single risk {MAX_SINGLE_RISK}%, peril concentration {MAX_PERIL_CONC}%, loss ratio threshold {MIN_LOSS_RATIO}, aggregate limit ${MAX_AGG_LIMIT/1e6:.0f}M.",
    table_properties={"quality": "gold"}
)
def gold_compliance_checks():
    policies = dlt.read("silver_enriched_policies")
    claims = dlt.read("silver_claims_enriched")

    bound = policies.filter(F.col("status") == "BOUND")
    total_gwp = bound.agg(F.sum("gross_premium")).collect()[0][0] or 1

    # Rule 1: Single risk concentration
    risk_check = (
        bound.groupBy("insured_name")
        .agg(
            F.sum("limit_amount").alias("total_limit"),
            F.sum("gross_premium").alias("total_premium"),
        )
        .withColumn("weight_pct", F.col("total_premium") / F.lit(total_gwp) * 100)
        .withColumn("rule_name", F.lit("Single Risk Limit"))
        .withColumn("threshold", F.lit(MAX_SINGLE_RISK))
        .withColumn("actual_value", F.col("weight_pct"))
        .withColumn("is_breached", F.col("weight_pct") > MAX_SINGLE_RISK)
        .withColumn("is_near_breach", (F.col("weight_pct") > MAX_SINGLE_RISK * 0.8) & ~(F.col("weight_pct") > MAX_SINGLE_RISK))
        .withColumn("entity", F.col("insured_name"))
        .select("rule_name", "threshold", "actual_value", "is_breached", "is_near_breach", "entity")
    )

    # Rule 2: Peril concentration
    peril_check = (
        bound.groupBy("peril")
        .agg(F.sum("gross_premium").alias("peril_premium"))
        .withColumn("weight_pct", F.col("peril_premium") / F.lit(total_gwp) * 100)
        .withColumn("rule_name", F.lit("Peril Concentration Limit"))
        .withColumn("threshold", F.lit(MAX_PERIL_CONC))
        .withColumn("actual_value", F.col("weight_pct"))
        .withColumn("is_breached", F.col("weight_pct") > MAX_PERIL_CONC)
        .withColumn("is_near_breach", (F.col("weight_pct") > MAX_PERIL_CONC * 0.8) & ~(F.col("weight_pct") > MAX_PERIL_CONC))
        .withColumn("entity", F.col("peril"))
        .select("rule_name", "threshold", "actual_value", "is_breached", "is_near_breach", "entity")
    )

    # Rule 3: Aggregate limit check
    agg_check = (
        bound.groupBy("territory")
        .agg(F.sum("limit_amount").alias("aggregate_limit"))
        .withColumn("rule_name", F.lit("Aggregate Limit"))
        .withColumn("threshold", F.lit(MAX_AGG_LIMIT))
        .withColumn("actual_value", F.col("aggregate_limit"))
        .withColumn("is_breached", F.col("aggregate_limit") > MAX_AGG_LIMIT)
        .withColumn("is_near_breach", (F.col("aggregate_limit") > MAX_AGG_LIMIT * 0.8) & ~(F.col("aggregate_limit") > MAX_AGG_LIMIT))
        .withColumn("entity", F.col("territory"))
        .select("rule_name", "threshold", "actual_value", "is_breached", "is_near_breach", "entity")
    )

    return risk_check.union(peril_check).union(agg_check) \
        .withColumn("check_date", F.current_date()) \
        .withColumn("severity",
            F.when(F.col("is_breached"), "CRITICAL")
             .when(F.col("is_near_breach"), "WARNING")
             .otherwise("CLEAR"))

# ═══════════════════════════════════════════
#  GOLD: Portfolio Exposure Summary
#  Territory, peril, LOB breakdown
# ═══════════════════════════════════════════

@dlt.table(
    comment="Portfolio exposure by territory, peril, and LOB with CAT PML aggregation.",
    table_properties={"quality": "gold"}
)
def gold_exposure_summary():
    policies = dlt.read("silver_enriched_policies")
    exposure = dlt.read("silver_exposure")

    bound = policies.filter(F.col("status") == "BOUND")

    # Policy-based exposure by territory + peril
    policy_exp = (
        bound.groupBy("territory", "peril", "lob_name")
        .agg(
            F.sum("limit_amount").alias("total_limit"),
            F.sum("gross_premium").alias("total_gwp"),
            F.sum("net_premium").alias("total_nwp"),
            F.count("*").alias("policy_count"),
        )
    )

    # Latest CAT model exposure
    latest_exp = (
        exposure
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("territory", "peril").orderBy(F.col("as_of_date").desc())))
        .filter(F.col("_rn") == 1)
        .select("territory", "peril",
            F.col("total_insured_value").alias("cat_tiv"),
            F.col("probable_max_loss").alias("cat_pml"),
            F.col("model_source").alias("cat_model"))
    )

    return policy_exp.join(latest_exp, ["territory", "peril"], "left") \
        .withColumn("as_of_date", F.current_date())

