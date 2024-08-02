import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.transforms import DropFields, SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.job import Job
import gs_now


args = getResolvedOptions(sys.argv,
    ["JOB_NAME", "SOURCE_BUCKET", "SOURCE_PREFIX", "TABLE_NAME", "TARGET_BUCKET",
     "TARGET_PREFIX", "DQ_BUCKET", "DQ_PREFIX"]
)

spark = (
    SparkSession.builder.appName("LandingzoneToTransicao")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.int96TimestampConversion", "true")
    .config("spark.sql.parquet.int96AsTimestamp", "false")
    .config("spark.sql.parquet.outputTimestampType", "INT96")
    .config("spark.sql.parquet.mergeSchema", "true")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
    .config('spark.sql.execution.arrow.pyspark.enabled', "true")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewedJoin.enabled", "true")
    .config('spark.sql.adaptive.coalescePartitions.enabled', "true")
    .config('spark.sql.adaptive.optimizerEnabled', "true")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext
glueContext = GlueContext(sc)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

####################################################################################################
###                                     PARAMETERS & VARIABLES                                   ###
####################################################################################################

TABLE_NAME: str = args["TABLE_NAME"]
SOURCE_BUCKET: str = args["SOURCE_BUCKET"]
SOURCE_PREFIX: str = args["SOURCE_PREFIX"]
DQ_BUCKET: str = args["DQ_BUCKET"]
DQ_PREFIX: str = args["DQ_PREFIX"]
TARGET_BUCKET: str = args["TARGET_BUCKET"]
TARGET_PREFIX: str = args["TARGET_PREFIX"]

table_input_path = f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/{TABLE_NAME.upper()}"
table_output_path = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/{TABLE_NAME}"
dq_output_path = f"s3://{DQ_BUCKET}/{DQ_PREFIX}"

####################################################################################################
###                                           READ DATA                                          ###
####################################################################################################

# Read data
dyf_input = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [table_input_path],
        "recurse": True,
    },
    transformation_ctx="dyf_input",
)

# Drop columns
dyf_droped = DropFields.apply(
    frame=dyf_input,
    paths=["_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_additional_properties"],
    transformation_ctx="dyf_droped",
)

# Create column
dyf_time = dyf_droped.gs_now(
    colName="_timestamp_crua",
    dateFormat="yyyy-MM-dd HH:mm:ss"
)

####################################################################################################
###                                          DATA QUALITY                                        ###
####################################################################################################

# Rules
DATAQUALITY_RULES = ""

evaluate_data_quality = EvaluateDataQuality().process_rows(
    frame=dyf_time,
    ruleset=DATAQUALITY_RULES,
    publishing_options={
        "dataQualityEvaluationContext": "evaluate_data_quality",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "performanceTuning.caching": "CACHE_INPUT",
        "observations.scope": "ALL",
    },
)

# RowLevelOutcomes
row_level_outcomes = SelectFromCollection.apply(
    dfc=evaluate_data_quality,
    key="rowLevelOutcomes",
    transformation_ctx="row_level_outcomes",
)

# RuleOutcomes
rule_outcomes = SelectFromCollection.apply(
    dfc=evaluate_data_quality,
    key="ruleOutcomes",
    transformation_ctx="rule_outcomes",
)

####################################################################################################
###                                           SAVE DATA                                          ###
####################################################################################################

df_table = row_level_outcomes.toDF()\
    .drop("DataQualityRulesPass", "DataQualityRulesSkip")

# Write table
df_table.write\
    .format("parquet")\
    .mode("overwrite")\
    .partitionBy(["dataqualityevaluationresult"])\
    .save(table_output_path)


df_dq = rule_outcomes.toDF()\
    .withColumn("ano", F.date_format(F.current_timestamp(), "yyyy"))\
    .withColumn("mes", F.date_format(F.current_timestamp(), "MM"))\
    .withColumn("validation_datetime", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

columns_order = [df_dq.columns[-1]] + df_dq.columns[:-1]

# Write data quality result
df_dq.select(columns_order)\
    .coalesce(1)\
    .write.format("parquet")\
    .mode("append")\
    .partitionBy(["ano", "mes"])\
    .save(dq_output_path)


job.commit()
