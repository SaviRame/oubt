import sys
import json
import datetime
from typing import Dict, Any, List

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def s3_path(bucket: str, key_or_prefix: str) -> str:
    return f"s3://{bucket}/{key_or_prefix.lstrip('/')}"


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def safe_bool(x: str, default: bool = False) -> bool:
    if x is None:
        return default
    return str(x).strip().lower() in ("true", "1", "yes", "y")


def require_columns(df: DataFrame, cols: List[str], label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise Exception(f"[{label}] Missing required columns: {missing}. Available: {df.columns}")


def write_json_report(glue_ctx: GlueContext, report: Dict[str, Any], out_path: str):
    spark = glue_ctx.spark_session
    df = spark.createDataFrame([json.dumps(report)], T.StringType()).toDF("json")
    df.coalesce(1).write.mode("overwrite").text(out_path)


# -------------------------
# Args
# -------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "LANDING_PREFIX",
        "OUT_PREFIX",
        "ZONE_FILE",
        "TXN_PREFIX",
        "ZONE_KEY_COL",
        "PU_COL",
        "DO_COL",
        "FAIL_ON_DQ",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
landing_prefix = args["LANDING_PREFIX"].rstrip("/") + "/"
out_prefix = args["OUT_PREFIX"].rstrip("/") + "/"

zone_file = args["ZONE_FILE"]
txn_prefix = args["TXN_PREFIX"].rstrip("/") + "/"

zone_key_col = args["ZONE_KEY_COL"]
pu_col = args["PU_COL"]
do_col = args["DO_COL"]
fail_on_dq = safe_bool(args["FAIL_ON_DQ"], True)

run_id = f"run_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

# Paths
zone_csv_path = s3_path(bucket, landing_prefix + zone_file)
txn_parquet_path = s3_path(bucket, landing_prefix + txn_prefix)

zones_out_path = s3_path(bucket, out_prefix + "zones_parquet/")
txns_valid_out_path = s3_path(bucket, out_prefix + "transactions_valid/")
quarantine_path = s3_path(bucket, out_prefix + "quarantine/")
dq_report_out_path = s3_path(bucket, out_prefix + f"dq-reports/{run_id}/")

print("=== INPUTS ===")
print("Zones CSV:", zone_csv_path)
print("Transactions Parquet prefix:", txn_parquet_path)
print("=== OUTPUTS ===")
print("Zones Parquet:", zones_out_path)
print("Transactions Valid:", txns_valid_out_path)
print("Quarantine:", quarantine_path)
print("DQ Report:", dq_report_out_path)

# -------------------------
# Read inputs
# -------------------------
zones_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(zone_csv_path)
)

txns_raw = spark.read.parquet(txn_parquet_path)

require_columns(zones_raw, [zone_key_col], "Zones")
require_columns(txns_raw, [pu_col, do_col], "Transactions")

# Standardize join types
zones = zones_raw.withColumn(zone_key_col, F.trim(F.col(zone_key_col).cast("string")))
txns = (
    txns_raw
    .withColumn(pu_col, F.trim(F.col(pu_col).cast("string")))
    .withColumn(do_col, F.trim(F.col(do_col).cast("string")))
)

# -------------------------
# DQ checks on zones
# -------------------------
scorecard = []
overall_pass = True

zones_total = zones.count()

zones_null_key = zones.filter(F.col(zone_key_col).isNull() | (F.length(F.col(zone_key_col)) == 0)).count()
passed = (zones_null_key == 0)
overall_pass = overall_pass and passed
scorecard.append({
    "dimension": "Completeness",
    "business_metric": f"{zone_key_col} null/empty count",
    "owner": "Data Engineering",
    "threshold": "= 0",
    "action_on_failure": "Fail job",
    "actual_value": zones_null_key,
    "passed": bool(passed),
})

zones_dup_groups = zones.groupBy(zone_key_col).count().filter(F.col("count") > 1).count()
passed = (zones_dup_groups == 0)
overall_pass = overall_pass and passed
scorecard.append({
    "dimension": "Uniqueness",
    "business_metric": f"Duplicate {zone_key_col} groups",
    "owner": "Data Owner",
    "threshold": "= 0",
    "action_on_failure": "Fail job",
    "actual_value": zones_dup_groups,
    "passed": bool(passed),
})

zones_invalid = zones.filter(F.col(zone_key_col).isNull() | (F.length(F.col(zone_key_col)) == 0))
zones_valid = zones.filter(~(F.col(zone_key_col).isNull() | (F.length(F.col(zone_key_col)) == 0)))

zone_keys = zones_valid.select(F.col(zone_key_col).alias("zkey")).dropDuplicates(["zkey"])

# -------------------------
# Referential Integrity: transactions -> zones
# -------------------------
tx1 = txns.join(zone_keys, txns[pu_col] == zone_keys["zkey"], "left") \
          .withColumn("pu_valid", F.col("zkey").isNotNull()) \
          .drop("zkey")

tx2 = tx1.join(zone_keys, tx1[do_col] == zone_keys["zkey"], "left") \
         .withColumn("do_valid", F.col("zkey").isNotNull()) \
         .drop("zkey")

orphan_pu = tx2.filter(~F.col("pu_valid")).count()
orphan_do = tx2.filter(~F.col("do_valid")).count()

passed_pu = (orphan_pu == 0)
passed_do = (orphan_do == 0)
overall_pass = overall_pass and passed_pu and passed_do

scorecard.append({
    "dimension": "Referential Integrity",
    "business_metric": f"Orphan trips: {pu_col} not in zones",
    "owner": "Governance",
    "threshold": "= 0",
    "action_on_failure": "Fail + quarantine",
    "actual_value": orphan_pu,
    "passed": bool(passed_pu),
})
scorecard.append({
    "dimension": "Referential Integrity",
    "business_metric": f"Orphan trips: {do_col} not in zones",
    "owner": "Governance",
    "threshold": "= 0",
    "action_on_failure": "Fail + quarantine",
    "actual_value": orphan_do,
    "passed": bool(passed_do),
})

txns_valid = tx2.filter(F.col("pu_valid") & F.col("do_valid")).drop("pu_valid", "do_valid")
txns_invalid = tx2.filter(~(F.col("pu_valid") & F.col("do_valid"))).drop("pu_valid", "do_valid")

# Debug counts 
print("DEBUG zones_valid count =", zones_valid.count())
print("DEBUG txns_valid count  =", txns_valid.count())
print("DEBUG txns_invalid count=", txns_invalid.count())

# -------------------------
# WRITE OUTPUTS  
# -------------------------
zones_valid.write.mode("overwrite").parquet(zones_out_path)
txns_valid.write.mode("overwrite").parquet(txns_valid_out_path)

zones_invalid.write.mode("overwrite").parquet(quarantine_path.rstrip("/") + "/zones_invalid/")
txns_invalid.write.mode("overwrite").parquet(quarantine_path.rstrip("/") + "/trips_orphan_location/")

dq_report = {
    "run_id": run_id,
    "job_name": args["JOB_NAME"],
    "timestamp_utc": now_utc_iso(),
    "inputs": {"zones_csv": zone_csv_path, "transactions_parquet_prefix": txn_parquet_path},
    "outputs": {
        "zones_parquet": zones_out_path,
        "transactions_valid": txns_valid_out_path,
        "quarantine": quarantine_path,
        "dq_report": dq_report_out_path,
    },
    "counts": {
        "zones_total": zones_total,
        "zones_invalid_key_rows": zones_null_key,
        "zones_duplicate_key_groups": zones_dup_groups,
        "transactions_total": txns.count(),
        "transactions_valid": txns_valid.count(),
        "transactions_invalid": txns_invalid.count(),
        "orphan_pickup": orphan_pu,
        "orphan_dropoff": orphan_do,
    },
    "scorecard": scorecard,
    "overall_pass": bool(overall_pass),
    "fail_on_dq": bool(fail_on_dq),
}

write_json_report(glueContext, dq_report, dq_report_out_path)

print("=== DQ REPORT (JSON) ===")
print(json.dumps(dq_report, indent=2))

if fail_on_dq and not overall_pass:
    raise Exception("QUALITY GATE FAILED. Check transformation-data/dq-reports/ for details.")

job.commit()
print("Job completed successfully.")
