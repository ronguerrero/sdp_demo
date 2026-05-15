# Databricks notebook source
# MAGIC %md
# MAGIC # Ascot Insurance Operations — Synthetic Data Generation
# MAGIC Generates realistic policy, claims, reinsurance, and exposure data for a specialty insurer.

# COMMAND ----------

dbutils.widgets.text("catalog", "ronguerrero")
dbutils.widgets.text("schema", "ascot_insurance_ops")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta, date
import random

# Lines of Business
lobs = [
    ("LOB-01","Property","Property & Casualty","Commercial property, catastrophe, builder's risk"),
    ("LOB-02","Casualty","Property & Casualty","General liability, professional liability, D&O"),
    ("LOB-03","Marine","Specialty","Cargo, hull, marine liability, war risks"),
    ("LOB-04","Energy","Specialty","Upstream, downstream, power generation, renewable"),
    ("LOB-05","Aviation","Specialty","Airlines, general aviation, space, satellite"),
    ("LOB-06","Cyber","Specialty","Data breach, ransomware, business interruption"),
    ("LOB-07","Political Risk","Specialty","Expropriation, contract frustration, trade credit"),
    ("LOB-08","Accident & Health","Health","Group life, personal accident, travel"),
]
lob_schema = StructType([
    StructField("lob_id",StringType()),StructField("lob_name",StringType()),
    StructField("segment",StringType()),StructField("description",StringType()),
])
spark.createDataFrame(lobs, lob_schema).write.mode("overwrite").saveAsTable("raw_lines_of_business")

# Brokers
brokers = [
    ("BRK-01","Marsh McLennan","Global","US","A+"),
    ("BRK-02","Aon","Global","US","A"),
    ("BRK-03","Willis Towers Watson","Global","GB","A"),
    ("BRK-04","Howden Group","London Market","GB","A-"),
    ("BRK-05","Lockton","Mid-Market","US","A"),
    ("BRK-06","Gallagher","Regional","US","A-"),
]
brk_schema = StructType([
    StructField("broker_id",StringType()),StructField("broker_name",StringType()),
    StructField("segment",StringType()),StructField("country",StringType()),
    StructField("rating",StringType()),
])
spark.createDataFrame(brokers, brk_schema).write.mode("overwrite").saveAsTable("raw_brokers")

# Reinsurers
reinsurers = [
    ("RE-01","Swiss Re","AAA","CH",0.40),
    ("RE-02","Munich Re","AA+","DE",0.35),
    ("RE-03","Lloyd's Syndicates","A+","GB",0.25),
    ("RE-04","Hannover Re","AA-","DE",0.30),
    ("RE-05","SCOR","A+","FR",0.20),
]
re_schema = StructType([
    StructField("reinsurer_id",StringType()),StructField("reinsurer_name",StringType()),
    StructField("credit_rating",StringType()),StructField("country",StringType()),
    StructField("cession_rate",DoubleType()),
])
spark.createDataFrame(reinsurers, re_schema).write.mode("overwrite").saveAsTable("raw_reinsurers")

print(f"Reference data: {len(lobs)} LOBs, {len(brokers)} brokers, {len(reinsurers)} reinsurers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policies (~20K)

# COMMAND ----------

import dbldatagen as dg

END = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START = END - timedelta(days=730)  # 2 years

lob_ids = [l[0] for l in lobs]
broker_ids = [b[0] for b in brokers]
territories = ["US","GB","EU","APAC","LATAM","ME","AF"]
perils = ["Fire","Windstorm","Flood","Earthquake","Cyber Attack","Ransomware","Collision","Theft","Liability","Professional Negligence","Political Violence","Supply Chain","Pandemic"]

pol_gen = (
    dg.DataGenerator(spark, name="policies", rows=20000, partitions=8)
    .withColumn("policy_id", "string", template=r"POL-\kkkkkkkk")
    .withColumn("lob_id", "string", values=lob_ids, random=True)
    .withColumn("broker_id", "string", values=broker_ids, random=True)
    .withColumn("insured_name", "string", template=r"Insured Corp \kkkk")
    .withColumn("territory", "string", values=territories, weights=[30,20,15,15,8,7,5], random=True)
    .withColumn("inception_date", "timestamp", begin=START.strftime("%Y-%m-%d %H:%M:%S"), end=END.strftime("%Y-%m-%d %H:%M:%S"), random=True)
    .withColumn("currency", "string", values=["USD","GBP","EUR"], weights=[50,30,20], random=True)
    .withColumn("gross_premium", "double", minValue=10000, maxValue=5000000, random=True)
    .withColumn("limit_amount", "double", minValue=100000, maxValue=100000000, random=True)
    .withColumn("deductible", "double", minValue=1000, maxValue=500000, random=True)
    .withColumn("peril", "string", values=perils, random=True)
    .withColumn("status", "string", values=["BOUND","BOUND","BOUND","QUOTED","EXPIRED","CANCELLED"], random=True)
)

df_pol = pol_gen.build()
df_pol = df_pol \
    .withColumn("expiry_date", F.date_add(F.to_date("inception_date"), 365)) \
    .withColumn("policy_id", F.when(F.rand(42) < 0.03, F.lit(None)).otherwise(F.col("policy_id"))) \
    .withColumn("gross_premium", F.when(F.rand(43) < 0.04, F.lit(-1.0) * F.abs(F.col("gross_premium"))).otherwise(F.col("gross_premium"))) \
    .withColumn("limit_amount", F.when(F.rand(44) < 0.02, F.lit(0.0)).otherwise(F.col("limit_amount"))) \
    .withColumn("lob_id", F.when(F.rand(45) < 0.03, F.lit("LOB-99")).otherwise(F.col("lob_id")))

df_pol.write.mode("overwrite").saveAsTable("raw_policies")
print(f"Policies: {df_pol.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims (~8K)

# COMMAND ----------

claim_statuses = ["OPEN","OPEN","OPEN","RESERVED","PAID","CLOSED","REOPENED","DENIED"]
claim_gen = (
    dg.DataGenerator(spark, name="claims", rows=8000, partitions=4)
    .withColumn("claim_id", "string", template=r"CLM-\kkkkkkkk")
    .withColumn("policy_id", "string", template=r"POL-\kkkkkkkk")
    .withColumn("loss_date", "timestamp", begin=START.strftime("%Y-%m-%d %H:%M:%S"), end=END.strftime("%Y-%m-%d %H:%M:%S"), random=True)
    .withColumn("notification_date", "timestamp", begin=START.strftime("%Y-%m-%d %H:%M:%S"), end=END.strftime("%Y-%m-%d %H:%M:%S"), random=True)
    .withColumn("peril", "string", values=perils, random=True)
    .withColumn("territory", "string", values=territories, weights=[30,20,15,15,8,7,5], random=True)
    .withColumn("incurred_amount", "double", minValue=1000, maxValue=10000000, random=True)
    .withColumn("paid_amount", "double", minValue=0, maxValue=5000000, random=True)
    .withColumn("reserve_amount", "double", minValue=0, maxValue=8000000, random=True)
    .withColumn("status", "string", values=claim_statuses, random=True)
    .withColumn("adjuster", "string", values=["J. Smith","A. Chen","M. Patel","R. Wilson","S. Brown","K. Lee"], random=True)
    .withColumn("cause_code", "string", values=["NAT-CAT","ATTRITIONAL","LARGE-LOSS","CATASTROPHE","FRAUD"], weights=[15,40,25,10,10], random=True)
)

df_claims = claim_gen.build()
df_claims = df_claims \
    .withColumn("claim_id", F.when(F.rand(50) < 0.02, F.lit(None)).otherwise(F.col("claim_id"))) \
    .withColumn("incurred_amount", F.when(F.rand(51) < 0.03, -F.abs(F.col("incurred_amount"))).otherwise(F.col("incurred_amount"))) \
    .withColumn("paid_amount", F.when(F.rand(52) < 0.04, F.col("paid_amount") * 2 + F.col("incurred_amount")).otherwise(F.col("paid_amount")))

df_claims.write.mode("overwrite").saveAsTable("raw_claims")
print(f"Claims: {df_claims.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exposure Accumulations (CAT exposure by territory/peril)

# COMMAND ----------

exposure_rows = []
for t in territories:
    for p in ["Windstorm","Earthquake","Flood","Fire","Cyber Attack"]:
        for q in range(8):  # 8 quarters
            dt = (END - timedelta(days=q*90)).date()
            tiv = random.uniform(50_000_000, 2_000_000_000)
            pml = tiv * random.uniform(0.05, 0.30)
            exposure_rows.append((f"EXP-{t}-{p[:4]}-Q{q}", t, p, dt, round(tiv, 2), round(pml, 2), "USD", random.choice(["RMS","AIR","Internal"])))

exp_schema = StructType([
    StructField("exposure_id",StringType()),StructField("territory",StringType()),
    StructField("peril",StringType()),StructField("as_of_date",DateType()),
    StructField("total_insured_value",DoubleType()),StructField("probable_max_loss",DoubleType()),
    StructField("currency",StringType()),StructField("model_source",StringType()),
])
spark.createDataFrame(exposure_rows, exp_schema).write.mode("overwrite").saveAsTable("raw_exposure_accumulations")
print(f"Exposure accumulations: {len(exposure_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

for t in ["raw_lines_of_business","raw_brokers","raw_reinsurers","raw_policies","raw_claims","raw_exposure_accumulations"]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{t}").collect()[0][0]
    print(f"  {t}: {cnt:,}")
print(f"\nAll source tables created in {CATALOG}.{SCHEMA}")

