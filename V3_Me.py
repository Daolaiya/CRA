import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import lower, current_timestamp
from spark_audit import instrument_spark
import uuid

#import gs_now
#import org.apache.spark.sql.functions.current_timestamp

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'
        ,'source_database'
        ,'target_database'
        ,'spoke_path'
        ,'max_parallel'
        ])

hub_database = args['source_database']
spoke_database = args['target_database']
max_param=args.get('max_parallel', '10')
spoke_path = args['spoke_path']

try:
    MAX_PARALLEL = int(max_param)
except ValueError:
    MAX_PARALLEL = 10

sc = SparkContext()
sc.setLogLevel('INFO')
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
logger.info("info message")
logger.warn("warn message")
logger.error("error message")

job.init(args['JOB_NAME'], args)
# job.init("parallel_iceberg_loader", {})

sts = boto3.client("sts")
identity = sts.get_caller_identity()
aws_region = "ca-central-1" # retrieve from?
catalog_name = "spark_catalog"
aws_account_id = identity["Account"]


ICEBERG_FORMAT_VERSION = "2" #default value


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html
glue = boto3.client('glue')

try:
    spark = SparkSession.builder \
       .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
       .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkSessionCatalog") \
       .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{spoke_path}") \
       .config(f"spark.sql.catalog.{catalog_name}.client.region",f"{aws_region}") \
       .config(f"spark.sql.catalog.{catalog_name}.glue.account-id",f"{aws_account_id}") \
       .config(f"spark.sql.catalog.{catalog_name}.glue.lakeformation-enabled","true") \
       .config(f"spark.sql.catalog.dropDirectoryBeforeTable.enabled", "true") \
       .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
       .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
       .config("spark.sql.defaultCatalog", catalog_name) \
       .getOrCreate()
    instrument_spark(spark)
    print(f"Spark session created: {spark}")
except Exception as err:
    print(f"Spark session failed: {err}")

# ==============================================================================
# Runtime identifiers
# ==============================================================================
RUN_ID = args.get("JOB_RUN_ID")
BATCH_ID = str(uuid.uuid4())

STATUS_TABLE = "table_load_status"

# ==============================================================================
# Temporal column naming convention
# A source table is temporal when both columns are present.
# ==============================================================================
# The manual list is no longer needed because both period columns identify temporal tables.
# TEMPORAL_TABLES = {
#     "tasmfcd",
#     "totyori",
#     "trapchg",
#     "ttasmtr",
#     "ttastus",
#     "ttinhb",
#     "ttintrc",
#     "ttlcbck",
#     "ttracct",
#     "ttrvsdi",
#     "ttxpln",
#     "ttxplnv"
# }
# # Reserved for future temporal processing enhancements
# These column names now define the convention used for automatic detection.
TEMPORAL_BEGIN_COLUMN = "sys_sdttm"
TEMPORAL_END_COLUMN = "sys_ndttm"
# ==============================================================================
# Helpers
# ==============================================================================
def table_exists_in_spark(database: str, table: str) -> bool:
    return spark.sql(f"SHOW TABLES IN `{database}` LIKE '{table}'").count() > 0

def is_read_permission_issue(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    tokens = [
        "accessdenied",
        "permission",
        "permission_denied",
        "not authorized",
        "lake formation",
        "table_or_view_not_found",
        "no such table",
        "cannot be found",
        "does not exist",
    ]
    return any(t in msg for t in tokens)

def get_pk_columns(database: str, table: str):
    desc = spark.sql(f"DESCRIBE TABLE `{database}`.`{table}`")
    return (
        desc.filter(lower(desc.comment).contains("primary key"))
            .select("col_name")
            .rdd.flatMap(lambda x: x)
            .collect()
    )

def get_pk_column_comments(database: str, table: str):
    desc = spark.sql(f"DESCRIBE TABLE `{database}`.`{table}`")
    rows = (
        desc.filter(lower(desc.comment).contains("primary key"))
            .select("col_name", "comment")
            .collect()
    )
    return {r["col_name"]: r["comment"] for r in rows}

def select_columns_excluding_dm(df):
    return [f"`{c}`" for c in df.columns if not c.upper().startswith("DM_")]

# ==============================================================================
# Ensure status table exists (ONCE)
# ==============================================================================
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{spoke_database}`.`{STATUS_TABLE}` (
        run_id STRING,
        batch_id STRING,
        table_name STRING,
        source_count BIGINT,
        target_count BIGINT,
        rec_ldttm TIMESTAMP
    )
    USING iceberg
    LOCATION '{spoke_path.rstrip("/")}/{STATUS_TABLE}'
""")

# ==============================================================================
# Discover all HUB tables
# ==============================================================================
all_source_tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN `{hub_database}`").collect()
    if not r["isTemporary"]
]

# ==============================================================================
# Per‑table processing (NO status writes here)
# ==============================================================================
def process_single_table(table_name: str):
    print(f"\n===== Processing {hub_database}.{table_name} =====")

    src_fqn = f"`{hub_database}`.`{table_name}`"
    tgt_fqn = f"`{spoke_database}`.`{table_name}`"

    if not table_exists_in_spark(spoke_database, table_name):
        return {
            "table_name": table_name,
            "status": "skipped_missing_target"
        }

    # --------------------------------------------------------------------------
    # Read HUB source ONCE (authoritative source count)
    # --------------------------------------------------------------------------
    try:
        df_source = spark.table(src_fqn)
        
        source_count = (
            spark.sql(f"""
                SELECT SUM(record_count)
                FROM `{hub_database}`.`{table_name}`.files
            """).first()[0]
        )
        
        if source_count is None:
            source_count = 0
            
    except Exception as e:
        return {
            "table_name": table_name,
            "status": "skipped_permission" if is_read_permission_issue(e) else "failed",
            "reason": str(e)
        }

    pk_cols = get_pk_columns(hub_database, table_name)
    select_expr = ", ".join(select_columns_excluding_dm(df_source))

    # Detect temporal tables from source columns and validate processing inputs.
    # Superseded table-name detection is retained below for reference.
    # Lowercasing the table name is no longer needed because detection examines columns.
    # Membership in the manual list is replaced by checking both temporal columns.
    source_columns = {c.lower(): c for c in df_source.columns}
    is_temporal_table = (
        TEMPORAL_BEGIN_COLUMN in source_columns
        and TEMPORAL_END_COLUMN in source_columns
    )

    if is_temporal_table and not pk_cols:
        logger.warn(f"Skipping temporal table {table_name}: no primary-key markers")
        return {
            "table_name": table_name,
            "status": "skipped_missing_pk",
            "source_count": source_count
        }

    if pk_cols:
        required_dm = {"DM_TIMESTAMP", "DM_SEQUENCE", "DM_OPERATION_TYPE"}
        missing_dm = required_dm - {c.upper() for c in df_source.columns}
        if missing_dm:
            logger.warn(
                f"Skipping {table_name}: missing DM columns {sorted(missing_dm)}"
            )
            return {
                "table_name": table_name,
                "status": "skipped_missing_dm",
                "source_count": source_count
            }

    # --------------------------------------------------------------------------
    # CASE 1: PK EXISTS + temporal columns -> temporal logic
    # --------------------------------------------------------------------------
    # table_name_lower = table_name.lower()
    # is_temporal_table = table_name_lower in TEMPORAL_TABLES
    
    if pk_cols and is_temporal_table:
    
        logger.info(
            f"Processing temporal active view for {table_name}"
        )
        # Bi-temporal logic:
        # Select the latest row for each key and period start, then filter its operation.
        # Add temporal begin column to the partition key if not already present
    
        temporal_pk_cols = pk_cols.copy()
        if TEMPORAL_BEGIN_COLUMN.lower() not in [
            c.lower() for c in temporal_pk_cols
        ]:
            # The literal name is replaced with the actual source-column spelling.
            # temporal_pk_cols.append(TEMPORAL_BEGIN_COLUMN)
            temporal_pk_cols.append(source_columns[TEMPORAL_BEGIN_COLUMN])
    
        pk_expr = ", ".join([f"`{c}`" for c in temporal_pk_cols])
        logger.info(f"Temporal PK columns for {table_name}: {temporal_pk_cols}"

        )
    
        # The old end-column expression used the configured spelling directly.
        # It is replaced below with the actual source-column spelling and SQL quoting.
        # {TEMPORAL_END_COLUMN} DESC
        df_final = spark.sql(f"""
            SELECT {select_expr}
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY {pk_expr}
                           ORDER BY
                               DM_TIMESTAMP DESC,
                               DM_SEQUENCE DESC,
                               CASE DM_OPERATION_TYPE
                                   WHEN 'I' THEN 1
                                   WHEN 'B' THEN 2
                                   WHEN 'A' THEN 3
                                   WHEN 'D' THEN 4
                               END DESC,
                               `{source_columns[TEMPORAL_END_COLUMN]}` DESC
                       ) RN
                FROM {src_fqn}
            ) s
            WHERE RN = 1
              AND DM_OPERATION_TYPE IN ('I','A')
        """) 
    # --------------------------------------------------------------------------
    # CASE 2: Non-Temporal Active View non-temporal tables → existing logic
    # --------------------------------------------------------------------------
    elif pk_cols:
        # This branch-specific check is no longer needed because the shared check
        # above validates DM columns for both temporal and non-temporal tables.
        # required_dm = {"DM_TIMESTAMP", "DM_SEQUENCE", "DM_OPERATION_TYPE"}
        # if not required_dm.issubset({c.upper() for c in df_source.columns}):
        #     return {
        #         "table_name": table_name,
        #         "status": "skipped_missing_dm",
        #         "source_count": source_count
        #     }

        logger.info(
            f"Processing non-temporal active view for {table_name}"
        )
    
        pk_expr = ", ".join([f"`{c}`" for c in pk_cols])
    
        df_final = spark.sql(f"""
            SELECT {select_expr}
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY {pk_expr}
                           ORDER BY DM_TIMESTAMP DESC,
                                    DM_SEQUENCE DESC,
                                    DM_OPERATION_TYPE ASC
                       ) RN
                FROM {src_fqn}
            ) s
            WHERE RN = 1
              AND DM_OPERATION_TYPE IN ('I','A')
        """)
    # --------------------------------------------------------------------------
    # CASE 3: NO PK -> DIRECT LOAD
    # --------------------------------------------------------------------------
    else:
    
        logger.info(
            f"Processing full load (no PK) for table {table_name}"
        )
    
        df_final = df_source.selectExpr(*select_columns_excluding_dm(df_source))

    #if not df_final.head(1):
    if df_final.limit(1).count() == 0:

        return {
            "table_name": table_name,
            "status": "skipped_zero_load",
            "source_count": source_count,
            "target_count": 0
        }

    # --------------------------------------------------------------------------
    # WRITE TARGET (SINGLE ICEBERG COMMIT)
    # --------------------------------------------------------------------------
    try:
        df_final.writeTo(tgt_fqn).overwritePartitions()
        
        target_count = (
            spark.sql(f"""
                SELECT SUM(record_count)
                FROM `{spoke_database}`.`{table_name}`.files
            """).first()[0]
        )
        
        if target_count is None:
            target_count = 0
    except Exception as e:
        return {
            "table_name": table_name,
            "status": "failed",
            "reason": str(e)
        }

    return {
        "table_name": table_name,
        "status": "success",
        "source_count": source_count,
        "target_count": target_count
    }

# ==============================================================================
# Execute in parallel (DATA ONLY)
# ==============================================================================
results = []
with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
    futures = {
        executor.submit(process_single_table, t): t
        for t in all_source_tables
    }
    for f in as_completed(futures):
        results.append(f.result())

# ==============================================================================
# 🚨 SINGLE‑THREADED STATUS WRITE (CRITICAL FIX)
# ==============================================================================
# Idempotent cleanup for this run
spark.sql(f"""
    DELETE FROM `{spoke_database}`.`{STATUS_TABLE}`
    WHERE run_id   = '{RUN_ID}'
      AND batch_id = '{BATCH_ID}'
""")

# Prepare status rows
status_rows = [
    (
        RUN_ID,
        BATCH_ID,
        r["table_name"],
        r.get("source_count", 0),
        r.get("target_count", 0),
    )
    for r in results
    if r.get("status") == "success"
]

if status_rows:
    status_df = spark.createDataFrame(
        status_rows,
        ["run_id", "batch_id", "table_name", "source_count", "target_count"]
    ).withColumn("rec_ldttm", F.current_timestamp())

    status_df.writeTo(
        f"`{spoke_database}`.`{STATUS_TABLE}`"
    ).append()
    
# ==============================================================================
# WRITE FULL JSON SUMMARY (SUCCESS + SKIPPED + FAILED)
# ==============================================================================

s3 = boto3.client("s3")

def parse_s3_path(s3_path):
    parts = s3_path.replace("s3://", "").split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""

summary_data = {
    "run_id": RUN_ID,
    "batch_id": BATCH_ID,

    "success": [
        {
            "table_name": r["table_name"],
            "target_count": r.get("target_count", 0)
        }
        for r in results if r.get("status") == "success"
    ],

    "skipped": [
        {
            "table_name": r["table_name"],
            "reason": r.get("status")
        }
        for r in results if r.get("status", "").startswith("skipped")
    ],

    "failed": [
        {
            "table_name": r["table_name"],
            "reason": r.get("reason", "unknown error")
        }
        for r in results if r.get("status") == "failed"
    ]
}

summary_path = f"{spoke_path.rstrip('/')}/summary/{RUN_ID}.json"
bucket, key = parse_s3_path(summary_path)

s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=json.dumps(summary_data, indent=2)
)

print(f"JSON summary written: {summary_path}")

# ==============================================================================
# Summary
# ==============================================================================
success_tables = [r["table_name"] for r in results if r.get("status") == "success"]
failed_tables  = [r["table_name"] for r in results if r.get("status") == "failed"]
skipped_rows   = [r for r in results if r.get("status", "").startswith("skipped")]
failed_rows = [r for r in results if r.get("status") == "failed"]

print("\n==================== RUN SUMMARY ====================")
print(f"Run ID   : {RUN_ID}")
print(f"Batch ID : {BATCH_ID}")
print(f"Tables   : {len(all_source_tables)}")
print(f"Success  : {len(success_tables)}")
print(f"Skipped  : {len(skipped_rows)}")
print(f"Failed   : {len(failed_tables)}")

# --- Skipped details ---
if skipped_rows:
    print("\nSkipped tables:")
    for r in skipped_rows:
        table = r.get("table_name", "<unknown>")
        reason = r.get("status")
        print(f"  - {table} ({reason})")

# --- Failed details ---
if failed_rows:
    print("\nFailed tables:")
    for r in failed_rows:
        table = r.get("table_name", "<unknown>")
        reason = r.get("reason", "No error message")
        print(f"  - {table} (reason: {reason})")

print("=====================================================\n")

job.commit()