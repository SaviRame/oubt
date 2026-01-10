import json
import time
import boto3

s3 = boto3.client("s3")
athena = boto3.client("athena")

# ---- Config (edit these)
SQL_BUCKET = "day11-12-sql-glue"
SQL_PREFIX = "sql"  # sql/setup, sql/transforms, sql/tests
ATHENA_DATABASE = "analytics"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT = "s3://day11-12-sql-glue/athena-query-results/"

# file execution order
SETUP_FILES = ["setup/00_raw_tables.sql"]
TRANSFORM_FILES = [
    "transforms/01_taxi_trip_enriched.sql",
    "transforms/02_daily_pickup_zone_metrics.sql",
]
TEST_FILES = [
    "tests/01_test_taxi_trip_enriched.sql",
    "tests/02_test_daily_pickup_zone_metrics.sql",
]

REPAIR_TABLES = [
    "analytics.taxi_trip_enriched",
    "analytics.daily_pickup_zone_metrics",
]


def read_s3_text(bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def run_athena_query(sql: str, database: str) -> str:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )
    return resp["QueryExecutionId"]


def wait_for_query(qid: str) -> None:
    while True:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
                raise RuntimeError(f"Athena query failed: {state}. Reason: {reason}")
            return
        time.sleep(2)


def get_query_results(qid: str):
    # returns rows as list[list[str]]
    paginator = athena.get_paginator("get_query_results")
    rows = []
    for page in paginator.paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue") for c in r["Data"]])
    return rows


def split_sql_statements(sql_text: str):
    """
    Athena executes one statement at a time.
    Keep it simple: split on semicolon.
    Assumption: your scripts use semicolons to end statements and don't contain complex procedural SQL.
    """
    parts = [p.strip() for p in sql_text.split(";")]
    return [p for p in parts if p]


def run_sql_file(relative_path: str, database: str):
    key = f"{SQL_PREFIX}/{relative_path}"
    sql_text = read_s3_text(SQL_BUCKET, key)

    for stmt in split_sql_statements(sql_text):
        qid = run_athena_query(stmt, database)
        wait_for_query(qid)


def run_test_file(relative_path: str, database: str):
    key = f"{SQL_PREFIX}/{relative_path}"
    sql_text = read_s3_text(SQL_BUCKET, key)

    # Tests should be a single SELECT (your UNION ALL pattern)
    stmt_list = split_sql_statements(sql_text)
    if len(stmt_list) != 1:
        raise RuntimeError(f"Test file must contain exactly 1 statement: {relative_path}")

    qid = run_athena_query(stmt_list[0], database)
    wait_for_query(qid)

    rows = get_query_results(qid)
    # rows[0] is header
    failures = []
    for r in rows[1:]:
        # expected columns: test_name, failing_rows
        if not r or len(r) < 2:
            continue
        test_name = r[0]
        failing_rows = int(r[1] or "0")
        if failing_rows > 0:
            failures.append((test_name, failing_rows))

    if failures:
        raise RuntimeError(f"Data quality tests failed: {failures}")


def main():
    print("Running SETUP...")
    for f in SETUP_FILES:
        run_sql_file(f, database="raw")  # setup creates raw tables

    print("Running TRANSFORMS...")
    for f in TRANSFORM_FILES:
        run_sql_file(f, database=ATHENA_DATABASE)

    print("Repairing partitions...")
    for t in REPAIR_TABLES:
        qid = run_athena_query(f"MSCK REPAIR TABLE {t}", database=ATHENA_DATABASE)
        wait_for_query(qid)

    print("Running TESTS...")
    for f in TEST_FILES:
        run_test_file(f, database=ATHENA_DATABASE)

    print("SUCCESS: All transforms + tests completed.")


if __name__ == "__main__":
    main()
