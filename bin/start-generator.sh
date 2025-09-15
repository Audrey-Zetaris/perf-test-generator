#!/bin/bash

#######################################
# Delta Lake Performance Test Generator
# Startup Script
#######################################

# Set script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

# Source configuration
if [ -f "$BASE_DIR/conf/generator.conf" ]; then
    source "$BASE_DIR/conf/generator.conf"
fi

# Default configurations (can be overridden by generator.conf or environment variables)
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
EXECUTOR_INSTANCES="${EXECUTOR_INSTANCES:-4}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
DRIVER_MAX_RESULT_SIZE="${DRIVER_MAX_RESULT_SIZE:-4g}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-200}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-200}"

# Additional Spark configurations (new)
SPARK_MAX_PARTITION_BYTES="${SPARK_MAX_PARTITION_BYTES:-134217728}"  # 128MB
SPARK_OPEN_COST_IN_BYTES="${SPARK_OPEN_COST_IN_BYTES:-4194304}"     # 4MB
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-8g}"
SPARK_DRIVER_MAX_RESULT_SIZE="${SPARK_DRIVER_MAX_RESULT_SIZE:-4g}"

# Adaptive Query Execution settings with defaults
SPARK_ADAPTIVE_ENABLED="${SPARK_CONF_spark_sql_adaptive_enabled:-true}"
SPARK_ADAPTIVE_COALESCE_PARTITIONS="${SPARK_CONF_spark_sql_adaptive_coalescePartitions_enabled:-true}"
SPARK_ADAPTIVE_SKEW_JOIN="${SPARK_CONF_spark_sql_adaptive_skewJoin_enabled:-true}"
SPARK_ADAPTIVE_LOCAL_SHUFFLE_READER="${SPARK_CONF_spark_sql_adaptive_localShuffleReader_enabled:-true}"

# Delta Lake specific configurations
DELTA_BASE_PATH="${DELTA_BASE_PATH:-/tmp/delta-lake-bmt}"
DAYS_TO_GENERATE="${DAYS_TO_GENERATE:-1}"
START_DATE="${START_DATE:-}"
GB_PER_BUCKET="${GB_PER_BUCKET:-20}"
PARTITIONS_PER_BUCKET="${PARTITIONS_PER_BUCKET:-100}"
RECORDS_PER_BUCKET="${RECORDS_PER_BUCKET:-20000000}"

# AWS S3 configurations (optional)
AWS_ACCESS_KEY="${AWS_ACCESS_KEY:-}"
AWS_SECRET_KEY="${AWS_SECRET_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
USE_IAM_ROLE="${USE_IAM_ROLE:-false}"
AWS_ROLE_ARN="${AWS_ROLE_ARN:-}"
AWS_ROLE_SESSION_NAME="${AWS_ROLE_SESSION_NAME:-delta-lake-bmt-session}"
S3_ENDPOINT="${S3_ENDPOINT:-https://s3.amazonaws.com}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="${S3_PREFIX:-delta-lake-bmt}"
S3_PATH_STYLE_ACCESS="${S3_PATH_STYLE_ACCESS:-false}"
S3_MULTIPART_SIZE="${S3_MULTIPART_SIZE:-104857600}"
S3_MULTIPART_THRESHOLD="${S3_MULTIPART_THRESHOLD:-2147483647}"
S3_FAST_UPLOAD="${S3_FAST_UPLOAD:-true}"
S3_FAST_UPLOAD_BUFFER="${S3_FAST_UPLOAD_BUFFER:-disk}"
S3_MAX_CONNECTIONS="${S3_MAX_CONNECTIONS:-100}"
S3_CONNECTION_TIMEOUT="${S3_CONNECTION_TIMEOUT:-200000}"
S3_SOCKET_TIMEOUT="${S3_SOCKET_TIMEOUT:-200000}"
S3_REQUEST_TIMEOUT="${S3_REQUEST_TIMEOUT:-0}"
S3_MAX_ERROR_RETRY="${S3_MAX_ERROR_RETRY:-10}"
S3_SSL_ENABLED="${S3_SSL_ENABLED:-true}"
S3_SSL_VERIFY="${S3_SSL_VERIFY:-true}"
S3_COMMITTER_ENABLED="${S3_COMMITTER_ENABLED:-true}"
S3_COMMITTER_NAME="${S3_COMMITTER_NAME:-directory}"
S3_COMMITTER_STAGING_DIR="${S3_COMMITTER_STAGING_DIR:-/tmp/spark-staging}"
S3_COMMITTER_CONFLICT_MODE="${S3_COMMITTER_CONFLICT_MODE:-replace}"
PARQUET_BLOCK_SIZE="${PARQUET_BLOCK_SIZE:-134217728}"
PARQUET_PAGE_SIZE="${PARQUET_PAGE_SIZE:-1048576}"
PARQUET_COMPRESSION="${PARQUET_COMPRESSION:-snappy}"
DELTA_LOG_STORE_CLASS="${DELTA_LOG_STORE_CLASS:-org.apache.spark.sql.delta.storage.S3SingleDriverLogStore}"
DELTA_CHECKPOINT_INTERVAL="${DELTA_CHECKPOINT_INTERVAL:-10}"

# Main JAR file
MAIN_JAR="$BASE_DIR/lib/perf-test-generator-1.0.0.jar"

# Check if main JAR exists
if [ ! -f "$MAIN_JAR" ]; then
    echo "ERROR: Main JAR not found at $MAIN_JAR"
    echo "Please run 'sbt assembly' and copy the JAR to lib/ directory"
    exit 1
fi

# Build classpath
CLASSPATH="$MAIN_JAR"
for jar in "$BASE_DIR"/lib/*.jar; do
    if [ "$jar" != "$MAIN_JAR" ]; then
        CLASSPATH="$CLASSPATH:$jar"
    fi
done

# Log configuration
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/generator-$(date +%Y%m%d-%H%M%S).log"

echo "========================================="
echo "Delta Lake Performance Test Generator"
echo "========================================="
echo "Configuration:"
echo "  Spark Master: $SPARK_MASTER"
echo "  Driver Memory: $DRIVER_MEMORY"
echo "  Executor Memory: $EXECUTOR_MEMORY"
echo "  Executor Instances: $EXECUTOR_INSTANCES"
echo "  Executor Cores: $EXECUTOR_CORES"
echo "  Data Path: $DELTA_BASE_PATH"
echo "  Days to Generate: $DAYS_TO_GENERATE"
if [ -n "$START_DATE" ]; then
    echo "  Start Date: $START_DATE"
else
    echo "  Start Date: Current time"
fi
echo "  GB per Bucket: $GB_PER_BUCKET"
echo "  Partitions per Bucket: $PARTITIONS_PER_BUCKET"
echo "  Log File: $LOG_FILE"
echo "========================================="

# Build Spark submit command
SPARK_SUBMIT_CMD="spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode client \
    --driver-memory $DRIVER_MEMORY \
    --executor-memory $EXECUTOR_MEMORY \
    --executor-cores $EXECUTOR_CORES \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.adaptive.enabled=$SPARK_ADAPTIVE_ENABLED \
    --conf spark.sql.adaptive.coalescePartitions.enabled=$SPARK_ADAPTIVE_COALESCE_PARTITIONS \
    --conf spark.sql.adaptive.skewJoin.enabled=$SPARK_ADAPTIVE_SKEW_JOIN \
    --conf spark.sql.adaptive.localShuffleReader.enabled=$SPARK_ADAPTIVE_LOCAL_SHUFFLE_READER \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS \
    --conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
    --conf spark.driver.maxResultSize=$DRIVER_MAX_RESULT_SIZE \
    --conf spark.sql.files.maxPartitionBytes=$SPARK_MAX_PARTITION_BYTES \
    --conf spark.sql.files.openCostInBytes=$SPARK_OPEN_COST_IN_BYTES"

# Add AWS configurations if provided
if [ -n "$S3_BUCKET" ]; then
    echo "  S3 Bucket: $S3_BUCKET"
    echo "  S3 Region: $S3_REGION"
    echo "  S3 Endpoint: $S3_ENDPOINT"
    
    # Update base path to use S3
    DELTA_BASE_PATH="s3a://$S3_BUCKET/$S3_PREFIX"
    echo "  S3 Path: $DELTA_BASE_PATH"
    
    # Core S3A filesystem configuration
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT \
        --conf spark.hadoop.fs.s3a.endpoint.region=$S3_REGION \
        --conf spark.hadoop.fs.s3a.path.style.access=$S3_PATH_STYLE_ACCESS"
    
    # Authentication configuration
    if [ "$USE_IAM_ROLE" = "true" ]; then
        echo "  Using IAM Role: $AWS_ROLE_ARN"
        SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider"
        
        if [ -n "$AWS_ROLE_ARN" ]; then
            SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
                --conf spark.hadoop.fs.s3a.assumed.role.arn=$AWS_ROLE_ARN \
                --conf spark.hadoop.fs.s3a.assumed.role.session.name=$AWS_ROLE_SESSION_NAME \
                --conf spark.hadoop.fs.s3a.assumed.role.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider"
        fi
    elif [ -n "$AWS_ACCESS_KEY" ] && [ -n "$AWS_SECRET_KEY" ]; then
        echo "  Using Access Key Authentication"
        SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
            --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY \
            --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_KEY"
        
        if [ -n "$AWS_SESSION_TOKEN" ]; then
            SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
                --conf spark.hadoop.fs.s3a.session.token=$AWS_SESSION_TOKEN"
        fi
    fi
    
    # Performance tuning settings
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
        --conf spark.hadoop.fs.s3a.fast.upload=$S3_FAST_UPLOAD \
        --conf spark.hadoop.fs.s3a.fast.upload.buffer=$S3_FAST_UPLOAD_BUFFER \
        --conf spark.hadoop.fs.s3a.multipart.size=$S3_MULTIPART_SIZE \
        --conf spark.hadoop.fs.s3a.multipart.threshold=$S3_MULTIPART_THRESHOLD \
        --conf spark.hadoop.fs.s3a.connection.maximum=$S3_MAX_CONNECTIONS \
        --conf spark.hadoop.fs.s3a.connection.timeout=$S3_CONNECTION_TIMEOUT \
        --conf spark.hadoop.fs.s3a.socket.timeout=$S3_SOCKET_TIMEOUT \
        --conf spark.hadoop.fs.s3a.attempts.maximum=$S3_MAX_ERROR_RETRY \
        --conf spark.hadoop.fs.s3a.retry.limit=$S3_MAX_ERROR_RETRY"
    
    # SSL configuration
    if [ "$S3_SSL_ENABLED" = "false" ]; then
        SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"
    fi
    
    # S3 committer configuration for optimized writes
    if [ "$S3_COMMITTER_ENABLED" = "true" ]; then
        SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
            --conf spark.hadoop.fs.s3a.committer.name=$S3_COMMITTER_NAME \
            --conf spark.hadoop.fs.s3a.committer.staging.dir=$S3_COMMITTER_STAGING_DIR \
            --conf spark.hadoop.fs.s3a.committer.staging.conflict-mode=$S3_COMMITTER_CONFLICT_MODE \
            --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
            --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"
    fi
    
    # Parquet configuration for S3
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
        --conf spark.hadoop.parquet.block.size=$PARQUET_BLOCK_SIZE \
        --conf spark.sql.parquet.compression.codec=$PARQUET_COMPRESSION \
        --conf spark.sql.parquet.mergeSchema=false \
        --conf spark.sql.parquet.filterPushdown=true \
        --conf spark.sql.hive.convertMetastoreParquet=false"
    
    # Delta Lake configuration for S3
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
        --conf spark.delta.logStore.class=$DELTA_LOG_STORE_CLASS \
        --conf spark.databricks.delta.checkpoint.writeStatsAsJson=false \
        --conf spark.databricks.delta.checkpoint.writeStatsAsStruct=true \
        --conf spark.databricks.delta.optimizeWrite.enabled=true \
        --conf spark.databricks.delta.autoCompact.enabled=true \
        --conf spark.databricks.delta.properties.defaults.checkpointInterval=$DELTA_CHECKPOINT_INTERVAL"
fi

# Add application arguments
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
    --class com.zetaris.testgen.DeltaLakeBMTApplication \
    $MAIN_JAR \
    --base-path $DELTA_BASE_PATH \
    --days $DAYS_TO_GENERATE \
    --gb-per-bucket $GB_PER_BUCKET \
    --partitions-per-bucket $PARTITIONS_PER_BUCKET \
    --records-per-bucket $RECORDS_PER_BUCKET"

# Add start date if provided
if [ -n "$START_DATE" ]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --start-date $START_DATE"
fi

# Add YARN/cluster specific configurations if not running locally
if [ "$SPARK_MASTER" != "local[*]" ] && [[ "$SPARK_MASTER" == yarn* ]]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
        --num-executors $EXECUTOR_INSTANCES \
        --conf spark.yarn.submit.waitAppCompletion=true \
        --conf spark.yarn.am.memory=2g \
        --conf spark.yarn.am.cores=2"
fi

echo ""
echo "Starting Spark application..."
echo "Command: $SPARK_SUBMIT_CMD"
echo ""

# Execute the Spark job
exec $SPARK_SUBMIT_CMD 2>&1 | tee "$LOG_FILE"