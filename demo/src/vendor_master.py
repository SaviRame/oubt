import sys

import pandas as pd
import recordlinkage as rl
from awsglue.utils import getResolvedOptions
from glue_utils import init_glue_job
from pyspark.sql import functions as F


DEBUG_COLUMNS = [
    "ingestion_date", "source_id_1", "source_id_2", "entity_type_1", "entity_type_2",
    "normalized_name_1", "normalized_name_2", "match_confidence",
    "match_group_1", "match_group_2", "same_group", "above_threshold",
]

# Match confidence thresholds
THRESHOLD_AUTO = 0.95           # Above this: automatic match
THRESHOLD_STEWARD = 0.85        # Above this: steward review
THRESHOLD_MATCH = 0.75          # Above this: considered a match


def get_glue_args(argv):
    return getResolvedOptions(
        argv,
        [
            "JOB_NAME",
            "bronze_db",
            "master_db",
            "debug_db",
            "ingestion_date",
            "output_path",
            "debug_output_path",
        ],
    )


def parse_args(argv):
    args = get_glue_args(argv)
    bronze_table = f"{args['bronze_db']}.vendor"
    master_dim_table = f"{args['master_db']}.dim_vendor"
    master_xref_table = f"{args['master_db']}.xref_vendor"
    debug_vendor_match_pairs_table = f"{args['debug_db']}.debug_vendor_match_pairs"
    ingestion_date = args["ingestion_date"]
    output_path = args["output_path"]
    debug_output_path = args["debug_output_path"]
    dim_vendor_path = f"{output_path}/dim_vendor"
    xref_vendor_path = f"{output_path}/xref_vendor"
    debug_vendor_match_pairs_path = f"{debug_output_path}/debug_vendor_match_pairs"
    return (
        args,
        bronze_table,
        master_dim_table,
        master_xref_table,
        debug_vendor_match_pairs_table,
        ingestion_date,
        dim_vendor_path,
        xref_vendor_path,
        debug_vendor_match_pairs_path,
    )


def init_spark(args):
    spark, glue_context, job = init_glue_job(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return spark, glue_context, job


def read_bronze(glue_context, bronze_table, ingestion_date):
    """
    Read from Glue Data Catalog, filtering by ingestion_date.
    """
    if "." not in bronze_table:
        raise ValueError(f"Expected database.table format, got: {bronze_table}")
    bronze_db, bronze_name = bronze_table.split(".", 1)

    df = glue_context.create_dynamic_frame.from_catalog(
        database=bronze_db, table_name=bronze_name
    ).toDF()
    return df.filter(F.col("ingestion_date") == F.lit(ingestion_date))


def normalized_vendor_name(col: F.Column) -> F.Column:
    """
    Normalize vendor name:
      - lowercase, trim
      - replace punctuation with spaces
      - collapse whitespace
      - remove legal suffixes (LLC, Inc, Ltd, Corp, etc.)
      - remove common business words that cause false matches
    """
    x = F.lower(F.trim(col))
    x = F.regexp_replace(x, r"[^a-z0-9]+", " ")
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)
    # Remove legal suffixes
    x = F.regexp_replace(
        x,
        r"\b(llc|l\.l\.c|inc|incorporated|ltd|limited|corp|corporation|co|company)\b",
        "",
    )
    # Remove common business words that cause false matches
    x = F.regexp_replace(
        x,
        r"\b(technologies|technology|tech|solutions|services|systems|group|holdings|enterprises)\b",
        "",
    )
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)
    return x


def transform_bronze(df):
    """
    Transform bronze data: type casting, null handling, normalization, deduplication.
    """
    df = df.select(
        F.col("vendor_id").cast("int").alias("vendor_id"),
        F.when(F.trim(F.col("vendor_name")) == "", F.lit(None))
        .otherwise(F.trim(F.col("vendor_name")))
        .alias("vendor_name"),
        F.col("ingestion_date").cast("date").alias("ingestion_date"),
    )
    df = df.withColumn("normalized_name", normalized_vendor_name(F.col("vendor_name")))
    return df.dropDuplicates(["vendor_id"])


def build_recordlinkage_metrics(vendor_df, existing_masters, ingestion_date=None):
    """
    Match new vendors against existing masters using Jaro-Winkler similarity.

    Returns:
        metrics_df: DataFrame with vendor_id, match_group, match_confidence
        debug_pairs_df: DataFrame with all pair comparisons for debugging
    """
    empty_result = (
        pd.DataFrame(columns=["vendor_id", "match_group", "match_confidence"]),
        pd.DataFrame(columns=DEBUG_COLUMNS),
    )

    new_vendors_pdf = (
        vendor_df.select("vendor_id", "vendor_name", "normalized_name")
        .dropna(subset=["vendor_id"]).toPandas()
    )
    if new_vendors_pdf.empty and existing_masters is None:
        return empty_result

    new_vendors_pdf = new_vendors_pdf.assign(
        vendor_id=lambda df: df["vendor_id"].astype("Int64"),
        entity_id=lambda df: "S:" + df["vendor_id"].astype(str),
        entity_type="S",
    )

    existing_masters_pdf = pd.DataFrame(columns=["vendor_gk", "normalized_name", "entity_id", "entity_type"])
    if existing_masters is not None:
        existing_masters_pdf = existing_masters.select("vendor_gk", "canonical_name", "normalized_name").toPandas()
        if not existing_masters_pdf.empty:
            existing_masters_pdf = existing_masters_pdf.assign(
                vendor_gk=lambda df: df["vendor_gk"].astype("Int64"),
                entity_id=lambda df: "G:" + df["vendor_gk"].astype(str),
                entity_type="G",
            )

    all_entities = pd.concat([
        new_vendors_pdf[["entity_id", "entity_type", "vendor_id", "normalized_name"]],
        existing_masters_pdf[["entity_id", "entity_type", "vendor_gk", "normalized_name"]],
    ], ignore_index=True)

    vendor_entities = new_vendors_pdf[["vendor_id", "entity_id"]].copy()
    matchable = all_entities.dropna(subset=["normalized_name"]).set_index("entity_id")
    if matchable.empty:
        return (
            vendor_entities.assign(match_group=-vendor_entities["vendor_id"].astype(int), match_confidence=0.0),
            empty_result[1],
        )

    # Compute pairwise Jaro-Winkler similarity
    indexer = rl.Index()
    indexer.full()
    compare = rl.Compare()
    compare.string("normalized_name", "normalized_name", method="jarowinkler", label="match_confidence")
    pairs_df = (
        compare.compute(indexer.index(matchable), matchable)
        .reset_index()
        .rename(columns={"level_0": "entity_id_1", "level_1": "entity_id_2"})
        .sort_values(["match_confidence", "entity_id_1", "entity_id_2"], ascending=[False, True, True])
    )

    # Union-Find to group matching entities
    parent = {eid: eid for eid in matchable.index}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # Union entities above threshold that involve at least one source vendor
    edges = pairs_df[
        (pairs_df["match_confidence"] >= THRESHOLD_MATCH)
        & (pairs_df["entity_id_1"].str.startswith("S:") | pairs_df["entity_id_2"].str.startswith("S:"))
    ]
    for a, b in edges[["entity_id_1", "entity_id_2"]].itertuples(index=False):
        union(a, b)

    entity_to_root = {eid: find(eid) for eid in parent}

    # Build root -> group mapping (prefer existing gk, else negative min vendor_id)
    root_to_gk, root_to_min_vid = {}, {}
    for row in matchable.reset_index().itertuples(index=False):
        root = entity_to_root.get(row.entity_id, row.entity_id)
        if row.entity_type == "G":
            root_to_gk[root] = min(int(row.vendor_gk), root_to_gk.get(root, int(row.vendor_gk)))
        else:
            root_to_min_vid[root] = min(int(row.vendor_id), root_to_min_vid.get(root, int(row.vendor_id)))

    root_to_group = {root: root_to_gk.get(root, -min_vid) for root, min_vid in root_to_min_vid.items()}

    # Map each vendor to its group
    def get_group(entity_id, vendor_id):
        root = entity_to_root.get(entity_id)
        return root_to_group.get(root, -int(vendor_id)) if root else -int(vendor_id)

    vendor_entities["match_group"] = vendor_entities.apply(
        lambda r: get_group(r["entity_id"], r["vendor_id"]), axis=1
    ).astype("int64")

    # Calculate best confidence for each vendor
    vendor_pairs = pairs_df[pairs_df["entity_id_1"].str.startswith("S:") | pairs_df["entity_id_2"].str.startswith("S:")]
    best_left = vendor_pairs.groupby("entity_id_1")["match_confidence"].max()
    best_right = vendor_pairs.groupby("entity_id_2")["match_confidence"].max()
    best_confidence = best_left.combine(best_right, max, fill_value=0.0)
    vendor_entities["match_confidence"] = vendor_entities["entity_id"].map(best_confidence).fillna(0.0)

    debug_pairs_df = build_debug_vendor_match_pairs(
        pairs_df, matchable, entity_to_root, root_to_group, ingestion_date
    )
    return vendor_entities[["vendor_id", "match_group", "match_confidence"]], debug_pairs_df


def build_debug_vendor_match_pairs(pairs_df, matchable, entity_to_root, root_to_group, ingestion_date):
    """Build debug dataframe with match pair details for analysis."""
    if pairs_df.empty:
        return pd.DataFrame(columns=DEBUG_COLUMNS)

    # Build lookup: entity_id -> {source_id, entity_type, normalized_name, match_group}
    entity_info = matchable.reset_index().assign(
        source_id=lambda df: df.apply(lambda r: r["vendor_id"] if r["entity_type"] == "S" else r["vendor_gk"], axis=1),
        match_group=lambda df: df["entity_id"].map(lambda eid: root_to_group.get(entity_to_root.get(eid, eid))),
    )
    lookup = entity_info.set_index("entity_id")[["source_id", "entity_type", "normalized_name", "match_group"]].to_dict("index")

    result = pairs_df[["match_confidence"]].assign(ingestion_date=ingestion_date)
    for suffix in ["_1", "_2"]:
        for field in ["source_id", "entity_type", "normalized_name", "match_group"]:
            result[f"{field}{suffix}"] = pairs_df[f"entity_id{suffix}"].map(lambda e, f=field: lookup.get(e, {}).get(f))

    result["same_group"] = (result["match_group_1"] == result["match_group_2"]) & result["match_group_1"].notna()
    result["above_threshold"] = result["match_confidence"] >= THRESHOLD_MATCH
    for col in ["match_group_1", "match_group_2", "source_id_1", "source_id_2"]:
        result[col] = result[col].astype("Int64")

    return result[DEBUG_COLUMNS]


def write_debug_vendor_match_pairs(df, spark, table, path):
    """
    Create debug_vendor_match_pairs table if missing and overwrite the data.
    Partitioned by ingestion_date.
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source_id_1 LONG,
            source_id_2 LONG,
            entity_type_1 STRING,
            entity_type_2 STRING,
            normalized_name_1 STRING,
            normalized_name_2 STRING,
            match_confidence DOUBLE,
            match_group_1 LONG,
            match_group_2 LONG,
            same_group BOOLEAN,
            above_threshold BOOLEAN,
            ingestion_date DATE
        )
        USING DELTA
        PARTITIONED BY (ingestion_date)
        LOCATION '{path}'
        """
    )
    (
        df.select(
            F.col("source_id_1").cast("long"),
            F.col("source_id_2").cast("long"),
            F.col("entity_type_1").cast("string"),
            F.col("entity_type_2").cast("string"),
            F.col("normalized_name_1").cast("string"),
            F.col("normalized_name_2").cast("string"),
            F.col("match_confidence").cast("double"),
            F.col("match_group_1").cast("long"),
            F.col("match_group_2").cast("long"),
            F.col("same_group").cast("boolean"),
            F.col("above_threshold").cast("boolean"),
            F.col("ingestion_date").cast("date"),
        )
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{df.first()['ingestion_date']}'")
        .save(path)
    )


def build_master_records_sql(spark, vendor_scored, existing_masters):
    """
    Build master records using SQL: survivorship + name comparison with existing masters.
    Returns DataFrame with vendor_gk, canonical_name, record_hash, match_group.
    """
    vendor_scored.createOrReplaceTempView("vendor_scored")

    # Create empty view if no existing masters (avoids duplicate SQL)
    if existing_masters is not None:
        existing_masters.createOrReplaceTempView("existing_masters")
    else:
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW existing_masters AS
            SELECT CAST(NULL AS LONG) AS vendor_gk, CAST(NULL AS STRING) AS canonical_name
            WHERE 1=0
        """)

    return spark.sql("""
        WITH survivorship_winner AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY match_group
                ORDER BY LENGTH(vendor_name) DESC, vendor_id ASC
            ) AS rn
            FROM vendor_scored
        ),
        group_winner AS (
            SELECT match_group, COALESCE(vendor_name, normalized_name) AS new_canonical
            FROM survivorship_winner WHERE rn = 1
        ),
        with_best_name AS (
            SELECT
                w.match_group,
                COALESCE(e.vendor_gk, ABS(XXHASH64(CONCAT_WS('|', w.new_canonical, CAST(w.match_group AS STRING))))) AS vendor_gk,
                CASE
                    WHEN e.vendor_gk IS NOT NULL AND LENGTH(w.new_canonical) > LENGTH(e.canonical_name) THEN w.new_canonical
                    WHEN e.vendor_gk IS NOT NULL THEN e.canonical_name
                    ELSE w.new_canonical
                END AS canonical_name
            FROM group_winner w
            LEFT JOIN existing_masters e ON w.match_group = e.vendor_gk
        )
        SELECT vendor_gk, canonical_name, SHA2(canonical_name, 256) AS record_hash, match_group
        FROM with_best_name
    """)


def merge_scd2_dim_vendor(spark, new_dim, table, path, ingestion_date):
    """Apply SCD Type 2 to dim_vendor using single MERGE statement."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            vendor_gk LONG,
            canonical_name STRING,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            change_reason STRING,
            record_hash STRING
        )
        USING DELTA
        LOCATION '{path}'
    """)

    new_dim.createOrReplaceTempView("new_dim")

    spark.sql(f"""
        MERGE INTO {table} AS t
        USING (
            SELECT n.vendor_gk, n.canonical_name, n.record_hash,
                   DATE '{ingestion_date}' AS valid_from,
                   CAST(NULL AS DATE) AS valid_to,
                   true AS is_current,
                   CASE WHEN e.vendor_gk IS NULL THEN 'NEW' ELSE 'RENAME' END AS change_reason,
                   1 AS _action
            FROM new_dim n
            LEFT JOIN {table} e ON n.vendor_gk = e.vendor_gk AND e.is_current
            WHERE e.vendor_gk IS NULL OR e.record_hash != n.record_hash

            UNION ALL

            SELECT e.vendor_gk, e.canonical_name, e.record_hash,
                   e.valid_from,
                   DATE_SUB(DATE '{ingestion_date}', 1) AS valid_to,
                   false AS is_current,
                   e.change_reason,
                   0 AS _action
            FROM new_dim n
            JOIN {table} e ON n.vendor_gk = e.vendor_gk AND e.is_current
            WHERE e.record_hash != n.record_hash
        ) AS s
        ON t.vendor_gk = s.vendor_gk AND t.is_current AND s._action = 0
        WHEN MATCHED THEN
            UPDATE SET valid_to = s.valid_to, is_current = false
        WHEN NOT MATCHED THEN
            INSERT (vendor_gk, canonical_name, valid_from, valid_to, is_current, change_reason, record_hash)
            VALUES (s.vendor_gk, s.canonical_name, s.valid_from, s.valid_to, s.is_current, s.change_reason, s.record_hash)
    """)


def merge_scd2_xref_vendor(spark, table, path, ingestion_date):
    """Apply SCD Type 2 to xref_vendor using single MERGE statement.

    Expects temp views: vendor_scored, master_records
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            vendor_id INT,
            vendor_gk LONG,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            decision STRING
        )
        USING DELTA
        LOCATION '{path}'
    """)

    # Build xref by joining vendor_scored with master_records
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW new_xref AS
        SELECT v.vendor_id, m.vendor_gk, v.decision
        FROM vendor_scored v
        LEFT JOIN master_records m ON v.match_group = m.match_group
    """)

    spark.sql(f"""
        MERGE INTO {table} AS t
        USING (
            SELECT n.vendor_id, n.vendor_gk, n.decision,
                   DATE '{ingestion_date}' AS valid_from,
                   CAST(NULL AS DATE) AS valid_to,
                   true AS is_current,
                   1 AS _action
            FROM new_xref n
            LEFT JOIN {table} e ON n.vendor_id = e.vendor_id AND e.is_current
            WHERE e.vendor_id IS NULL OR e.vendor_gk != n.vendor_gk OR e.decision != n.decision

            UNION ALL

            SELECT e.vendor_id, e.vendor_gk, e.decision,
                   e.valid_from,
                   DATE_SUB(DATE '{ingestion_date}', 1) AS valid_to,
                   false AS is_current,
                   0 AS _action
            FROM new_xref n
            JOIN {table} e ON n.vendor_id = e.vendor_id AND e.is_current
            WHERE e.vendor_gk != n.vendor_gk OR e.decision != n.decision
        ) AS s
        ON t.vendor_id = s.vendor_id AND t.is_current AND s._action = 0
        WHEN MATCHED THEN
            UPDATE SET valid_to = s.valid_to, is_current = false
        WHEN NOT MATCHED THEN
            INSERT (vendor_id, vendor_gk, valid_from, valid_to, is_current, decision)
            VALUES (s.vendor_id, s.vendor_gk, s.valid_from, s.valid_to, s.is_current, s.decision)
    """)


def main():
    (
        args,
        bronze_table,
        master_dim_table,
        master_xref_table,
        debug_vendor_match_pairs_table,
        ingestion_date,
        dim_vendor_path,
        xref_vendor_path,
        debug_vendor_match_pairs_path,
    ) = parse_args(sys.argv)
    spark, glue_context, job = init_spark(args)

    # Read from bronze and transform
    bronze_df = read_bronze(glue_context, bronze_table, ingestion_date)
    vendor_df = transform_bronze(bronze_df)

    if vendor_df.limit(1).count() == 0:
        job.commit()
        return

    # Load existing masters for matching
    existing_dim = (
        spark.read.table(master_dim_table)
        if spark.catalog.tableExists(master_dim_table)
        else None
    )
    existing_masters = None
    if existing_dim is not None:
        existing_masters = (
            existing_dim.filter(F.col("is_current"))
            .withColumn(
                "normalized_name", normalized_vendor_name(F.col("canonical_name"))
            )
            .select("vendor_gk", "canonical_name", "normalized_name")
        )

    # Match vendors to existing masters
    metrics_df, debug_pairs_df = build_recordlinkage_metrics(
        vendor_df, existing_masters, ingestion_date
    )
    metrics_sdf = spark.createDataFrame(metrics_df).select(
        F.col("vendor_id").cast("int"),
        F.col("match_group").cast("long"),
        F.col("match_confidence").cast("double"),
    )

    # Score vendors and determine decision
    vendor_scored = vendor_df.join(metrics_sdf, on="vendor_id", how="left")
    vendor_scored = vendor_scored.withColumn(
        "decision",
        F.when(F.col("match_confidence") > 0.95, F.lit("AUTO"))
        .when(F.col("match_confidence") >= 0.85, F.lit("STEWARD_REVIEW"))
        .when(F.col("match_confidence") >= 0.75, F.lit("MANUAL_REVIEW"))
        .otherwise(F.lit("NO_MATCH")),
    )

    master_records = build_master_records_sql(spark, vendor_scored, existing_masters)
    master_records.createOrReplaceTempView("master_records")

    # SCD2 merge for dim_vendor
    merge_scd2_dim_vendor(spark, master_records, master_dim_table, dim_vendor_path, ingestion_date)

    # SCD2 merge for xref_vendor (uses vendor_scored and master_records temp views)
    merge_scd2_xref_vendor(spark, master_xref_table, xref_vendor_path, ingestion_date)

    if not debug_pairs_df.empty:
        debug_pairs_sdf = spark.createDataFrame(debug_pairs_df)
        write_debug_vendor_match_pairs(
            debug_pairs_sdf,
            spark,
            debug_vendor_match_pairs_table,
            debug_vendor_match_pairs_path,
        )

    job.commit()


if __name__ == "__main__":
    main()
