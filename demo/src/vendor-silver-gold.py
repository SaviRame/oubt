import sys

import pandas as pd
import recordlinkage as rl
from awsglue.utils import getResolvedOptions
from glue_utils import init_glue_job
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_glue_args_gold(argv):
    return getResolvedOptions(
        argv,
        ["JOB_NAME", "silver_db", "gold_db", "ingestion_date", "write_mode"],
    )


def parse_args(argv):
    args = get_glue_args_gold(argv)
    silver_table = f"{args['silver_db']}.vendor"
    gold_dim_table = f"{args['gold_db']}.dim_vendor"
    gold_xref_table = f"{args['gold_db']}.xref_vendor"
    ingestion_date = args["ingestion_date"]
    return args, silver_table, gold_dim_table, gold_xref_table, ingestion_date


def init_spark(args):
    spark, glue_context, job = init_glue_job(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return spark, glue_context, job


def read_silver(spark, silver_table, ingestion_date):
    return spark.read.table(silver_table).filter(
        F.col("ingestion_date") == F.lit(ingestion_date).cast("date")
    )


def normalized_vendor_name(col: F.Column) -> F.Column:
    x = F.lower(F.trim(col))
    x = F.regexp_replace(x, r"[^a-z0-9]+", " ")
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)
    x = F.regexp_replace(
        x,
        r"\b(llc|l\.l\.c|inc|incorporated|ltd|limited|corp|corporation|co|company)\b",
        "",
    )
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)
    return x


def generate_record_hash(df):
    return df.withColumn(
        "record_hash", F.sha2(F.concat_ws("|", F.col("normalized_name")), 256)
    )


def build_recordlinkage_metrics(silver_df, existing_current):
    pdf_all = (
        silver_df.select("vendor_id", "vendor_name", "normalized_name", "record_hash")
        .dropna(subset=["vendor_id"])
        .toPandas()
    )

    if pdf_all.empty and existing_current is None:
        return pd.DataFrame(
            columns=["vendor_id", "match_group", "match_confidence", "match_rule"]
        )

    pdf_all["vendor_id"] = pdf_all["vendor_id"].astype(int)
    pdf_all["entity_id"] = "S:" + pdf_all["vendor_id"].astype(str)
    pdf_all["entity_type"] = "S"

    existing_pdf_all = pd.DataFrame(
        columns=[
            "vendor_gk",
            "canonical_name",
            "normalized_name",
            "entity_id",
            "entity_type",
        ]
    )
    if existing_current is not None:
        existing_pdf_all = existing_current.select(
            "vendor_gk", "canonical_name", "normalized_name"
        ).toPandas()
        if not existing_pdf_all.empty:
            existing_pdf_all["vendor_gk"] = existing_pdf_all["vendor_gk"].astype(int)
            existing_pdf_all["entity_id"] = "G:" + existing_pdf_all["vendor_gk"].astype(
                str
            )
            existing_pdf_all["entity_type"] = "G"

    combined = pd.concat(
        [
            pdf_all[["entity_id", "entity_type", "vendor_id", "normalized_name"]],
            existing_pdf_all[
                ["entity_id", "entity_type", "vendor_gk", "normalized_name"]
            ],
        ],
        ignore_index=True,
    )

    all_ids = pdf_all["vendor_id"].astype(int).tolist()
    silver_entities = pdf_all[["vendor_id", "entity_id"]].copy()

    matchable = combined.dropna(subset=["normalized_name"]).set_index("entity_id")
    if matchable.empty:
        metrics_df = pd.DataFrame({"vendor_id": all_ids})
        metrics_df["match_group"] = metrics_df["vendor_id"].map(lambda v: -int(v))
        metrics_df["match_confidence"] = 0.0
        metrics_df["match_rule"] = "RECORDLINKAGE"
        return metrics_df

    pairs_df = pd.DataFrame(columns=["vendor_id_1", "vendor_id_2", "match_confidence"])
    indexer = rl.Index()
    indexer.full()
    pairs = indexer.index(matchable)

    compare = rl.Compare()
    compare.string(
        "normalized_name",
        "normalized_name",
        method="jarowinkler",
        label="match_confidence",
    )
    features = compare.compute(pairs, matchable)
    pairs_df = (
        features.reset_index()
        .rename(columns={"level_0": "entity_id_1", "level_1": "entity_id_2"})
        .sort_values(
            ["match_confidence", "entity_id_1", "entity_id_2"],
            ascending=[False, True, True],
        )
    )

    parent = {eid: eid for eid in matchable.index.tolist()}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    edges = pairs_df[
        (pairs_df["match_confidence"] >= 0.75)
        & (
            pairs_df["entity_id_1"].str.startswith("S:")
            | pairs_df["entity_id_2"].str.startswith("S:")
        )
    ][["entity_id_1", "entity_id_2"]]
    for a, b in edges.itertuples(index=False):
        union(a, b)

    entity_to_root = {eid: find(eid) for eid in parent}

    root_to_gk = {}
    root_to_min_silver = {}
    matchable_reset = matchable.reset_index()
    for row in matchable_reset.itertuples(index=False):
        entity_id = row.entity_id
        root = entity_to_root.get(entity_id, entity_id)
        if row.entity_type == "G":
            gk = int(row.vendor_gk)
            root_to_gk[root] = min(gk, root_to_gk.get(root, gk))
        else:
            vid = int(row.vendor_id)
            root_to_min_silver[root] = min(vid, root_to_min_silver.get(root, vid))

    root_to_group = {}
    for root, min_vid in root_to_min_silver.items():
        if root in root_to_gk:
            root_to_group[root] = root_to_gk[root]
        else:
            root_to_group[root] = -min_vid

    def group_id_for_entity(entity_id, vendor_id):
        if entity_id in entity_to_root:
            root = entity_to_root[entity_id]
            return root_to_group.get(root, -int(vendor_id))
        return -int(vendor_id)

    groups_df = silver_entities.copy()
    groups_df["match_group"] = groups_df.apply(
        lambda row: group_id_for_entity(row["entity_id"], row["vendor_id"]), axis=1
    )

    best = pd.Series(0.0, index=silver_entities["entity_id"].tolist())
    if not pairs_df.empty:
        pairs_for_best = pairs_df[
            pairs_df["entity_id_1"].str.startswith("S:")
            | pairs_df["entity_id_2"].str.startswith("S:")
        ]
        best_left = pairs_for_best.groupby("entity_id_1")["match_confidence"].max()
        best_right = pairs_for_best.groupby("entity_id_2")["match_confidence"].max()
        best = best_left.combine(best_right, max).reindex(
            silver_entities["entity_id"].tolist(), fill_value=0.0
        )

    metrics_df = groups_df.assign(
        match_confidence=groups_df["entity_id"].map(best).fillna(0.0),
    )
    metrics_df["match_rule"] = metrics_df["match_confidence"].map(
        lambda c: "EXACT" if c == 1.0 else "RECORDLINKAGE"
    )
    return metrics_df[["vendor_id", "match_group", "match_confidence", "match_rule"]]


def apply_survivorship_rules(vendor_scored):
    window = Window.partitionBy("match_group").orderBy(
        F.desc(F.length("vendor_name")), F.asc("vendor_id")
    )
    return (
        vendor_scored.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def build_golden_records(vendor_scored, existing_current):
    canonical_name = F.coalesce(F.col("vendor_name"), F.col("normalized_name"))
    if existing_current is None:
        return vendor_scored.select(
            F.abs(
                F.xxhash64(
                    F.concat_ws(
                        "|", canonical_name, F.col("match_group").cast("string")
                    )
                )
            )
            .cast("long")
            .alias("vendor_gk"),
            canonical_name.alias("canonical_name"),
            F.col("record_hash"),
            F.col("match_group"),
        )

    existing_gk = existing_current.select(
        F.col("vendor_gk").alias("existing_vendor_gk")
    )
    joined = vendor_scored.join(
        existing_gk,
        vendor_scored["match_group"] == existing_gk["existing_vendor_gk"],
        "left",
    )
    return joined.select(
        F.when(F.col("existing_vendor_gk").isNotNull(), F.col("existing_vendor_gk"))
        .otherwise(
            F.abs(
                F.xxhash64(
                    F.concat_ws(
                        "|", canonical_name, F.col("match_group").cast("string")
                    )
                )
            )
        )
        .cast("long")
        .alias("vendor_gk"),
        canonical_name.alias("canonical_name"),
        F.col("record_hash"),
        F.col("match_group"),
    )


def apply_scd_type_2(new_dim, existing_dim, ingestion_date):
    new_dim_base = new_dim.select(
        "vendor_gk",
        "canonical_name",
        F.lit(ingestion_date).cast("date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.lit("NEW").alias("change_reason"),
        "record_hash",
    )

    if existing_dim is None:
        return new_dim_base

    existing_current = existing_dim.filter(F.col("is_current") == F.lit(True))
    existing_hist = existing_dim.filter(F.col("is_current") == F.lit(False))

    joined = new_dim_base.alias("n").join(
        existing_current.alias("e"), "vendor_gk", "left"
    )
    unchanged = joined.filter(
        F.col("e.vendor_gk").isNotNull()
        & (F.col("n.record_hash") == F.col("e.record_hash"))
    ).select("e.*")

    changed = joined.filter(
        F.col("e.vendor_gk").isNotNull()
        & (F.col("n.record_hash") != F.col("e.record_hash"))
    )
    closed = (
        changed.select("e.*")
        .withColumn("valid_to", F.date_sub(F.lit(ingestion_date).cast("date"), 1))
        .withColumn("is_current", F.lit(False))
        .withColumn("change_reason", F.lit("RENAME"))
    )
    changed_new = (
        changed.select("n.*")
        .withColumn("change_reason", F.lit("RENAME"))
        .withColumn("is_current", F.lit(True))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("valid_from", F.lit(ingestion_date).cast("date"))
    )

    new_only = joined.filter(F.col("e.vendor_gk").isNull()).select("n.*")

    return (
        existing_hist.unionByName(unchanged)
        .unionByName(closed)
        .unionByName(changed_new)
        .unionByName(new_only)
    )


def build_xref(vendor_scored, group_to_gk, ingestion_date):
    return vendor_scored.join(group_to_gk, on="match_group", how="left").select(
        F.col("vendor_id"),
        F.col("vendor_gk"),
        F.lit(ingestion_date).cast("date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.col("match_rule"),
        F.col("match_confidence"),
        F.col("decision"),
    )


def apply_xref_scd2(new_xref, existing_xref, ingestion_date):
    if existing_xref is None:
        return new_xref

    existing_current = existing_xref.filter(F.col("is_current") == F.lit(True))
    existing_hist = existing_xref.filter(F.col("is_current") == F.lit(False))

    joined = new_xref.alias("n").join(existing_current.alias("e"), "vendor_id", "left")
    unchanged = joined.filter(
        F.col("e.vendor_id").isNotNull()
        & (F.col("e.vendor_gk") == F.col("n.vendor_gk"))
        & (F.col("e.match_rule") == F.col("n.match_rule"))
        & (F.col("e.match_confidence") == F.col("n.match_confidence"))
        & (F.col("e.decision") == F.col("n.decision"))
    ).select("e.*")

    changed = joined.filter(
        F.col("e.vendor_id").isNotNull()
        & ~(
            (F.col("e.vendor_gk") == F.col("n.vendor_gk"))
            & (F.col("e.match_rule") == F.col("n.match_rule"))
            & (F.col("e.match_confidence") == F.col("n.match_confidence"))
            & (F.col("e.decision") == F.col("n.decision"))
        )
    )
    closed = (
        changed.select("e.*")
        .withColumn("valid_to", F.date_sub(F.lit(ingestion_date).cast("date"), 1))
        .withColumn("is_current", F.lit(False))
    )
    changed_new = changed.select("n.*")

    new_only = joined.filter(F.col("e.vendor_id").isNull()).select("n.*")

    return (
        existing_hist.unionByName(unchanged)
        .unionByName(closed)
        .unionByName(changed_new)
        .unionByName(new_only)
    )


def write_gold_table(df, table, spark):
    df.write.format("delta").mode("overwrite").saveAsTable(table)


def main():
    args, silver_table, gold_dim_table, gold_xref_table, ingestion_date = parse_args(
        sys.argv
    )
    spark, glue_context, job = init_spark(args)

    silver_df = read_silver(spark, silver_table, ingestion_date)
    if silver_df.limit(1).count() == 0:
        job.commit()
        return

    silver_df = generate_record_hash(silver_df)

    existing_dim = (
        spark.read.table(gold_dim_table)
        if spark.catalog.tableExists(gold_dim_table)
        else None
    )
    existing_current = None
    if existing_dim is not None:
        existing_current = (
            existing_dim.filter(F.col("is_current") == F.lit(True))
            .withColumn(
                "normalized_name", normalized_vendor_name(F.col("canonical_name"))
            )
            .select("vendor_gk", "canonical_name", "normalized_name")
        )

    metrics_df = build_recordlinkage_metrics(silver_df, existing_current)
    metrics_sdf = spark.createDataFrame(metrics_df).select(
        F.col("vendor_id").cast("int"),
        F.col("match_group").cast("long"),
        F.col("match_confidence").cast("double"),
        F.col("match_rule").cast("string"),
    )

    silver_scored = silver_df.join(metrics_sdf, on="vendor_id", how="left")
    silver_scored = silver_scored.withColumn(
        "decision",
        F.when(F.col("match_confidence") > 0.95, F.lit("AUTO"))
        .when(F.col("match_confidence") >= 0.85, F.lit("STEWARD_REVIEW"))
        .when(F.col("match_confidence") >= 0.75, F.lit("MANUAL_REVIEW"))
        .otherwise(F.lit("NO_MATCH")),
    )

    golden_candidates = apply_survivorship_rules(silver_scored)
    golden_records = build_golden_records(golden_candidates, existing_current)

    final_dim_vendor = apply_scd_type_2(golden_records, existing_dim, ingestion_date)

    group_to_gk = golden_records.select("match_group", "vendor_gk")
    new_xref = build_xref(silver_scored, group_to_gk, ingestion_date)
    existing_xref = (
        spark.read.table(gold_xref_table)
        if spark.catalog.tableExists(gold_xref_table)
        else None
    )
    final_xref_vendor = apply_xref_scd2(new_xref, existing_xref, ingestion_date)

    write_gold_table(final_dim_vendor, gold_dim_table, spark)
    write_gold_table(final_xref_vendor, gold_xref_table, spark)

    job.commit()


if __name__ == "__main__":
    main()
