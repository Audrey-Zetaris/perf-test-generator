# S3 Configuration Guide

This guide explains how to configure the Delta Lake Performance Test Generator to write data to Amazon S3 or S3-compatible storage.

## Quick Start

1. Copy the example configuration:
```bash
cp conf/s3-example.conf conf/generator.conf
```

2. Edit `conf/generator.conf` with your S3 credentials and bucket:
```bash
vi conf/generator.conf
```

3. Run the generator:
```bash
./bin/start-generator.sh
```

## Configuration Options

### Authentication Methods

#### Option 1: Access Keys (Recommended for Development)
```bash
AWS_ACCESS_KEY="your-access-key-id"
AWS_SECRET_KEY="your-secret-access-key"
```

#### Option 2: IAM Role (Recommended for Production on EC2)
```bash
USE_IAM_ROLE="true"
# Optionally specify a role to assume
AWS_ROLE_ARN="arn:aws:iam::123456789012:role/YourRole"
```

#### Option 3: Temporary Credentials
```bash
AWS_ACCESS_KEY="your-access-key-id"
AWS_SECRET_KEY="your-secret-access-key"
AWS_SESSION_TOKEN="your-session-token"
```

### S3 Bucket Configuration

```bash
# Required: Your S3 bucket name
S3_BUCKET="your-bucket-name"

# Optional: Folder within the bucket (default: delta-lake-bmt)
S3_PREFIX="delta-lake-bmt"

# AWS Region (default: us-east-1)
S3_REGION="us-east-1"
```

### S3-Compatible Storage (MinIO, Ceph, etc.)

For S3-compatible storage systems:

```bash
S3_ENDPOINT="https://your-minio-endpoint.com"
S3_PATH_STYLE_ACCESS="true"  # Required for most S3-compatible systems
S3_SSL_ENABLED="true"  # Set to false if using HTTP
```

## Performance Optimization

### For Large-Scale Data Generation (23TB)

The default configuration generates 23TB of data (20GB × 1,152 buckets). Optimize for this scale:

#### Memory Configuration
```bash
DRIVER_MEMORY="32g"
EXECUTOR_MEMORY="32g"
EXECUTOR_INSTANCES="20"
EXECUTOR_CORES="8"
```

#### Parallelism Settings
```bash
SPARK_SHUFFLE_PARTITIONS="800"
SPARK_DEFAULT_PARALLELISM="800"
PARTITIONS_PER_BUCKET="100"
```

#### S3 Upload Optimization
```bash
# Larger multipart uploads for better throughput
S3_MULTIPART_SIZE="268435456"  # 256MB parts
S3_MULTIPART_THRESHOLD="268435456"  # Use multipart for files > 256MB

# Increase connection pool
S3_MAX_CONNECTIONS="400"
S3_CONNECTION_TIMEOUT="600000"  # 10 minutes
S3_SOCKET_TIMEOUT="600000"  # 10 minutes

# Fast upload with disk buffering (more stable for large files)
S3_FAST_UPLOAD="true"
S3_FAST_UPLOAD_BUFFER="disk"
```

#### Parquet Optimization for S3
```bash
# Larger blocks for S3 (reduces number of S3 requests)
PARQUET_BLOCK_SIZE="536870912"  # 512MB
PARQUET_PAGE_SIZE="10485760"  # 10MB
PARQUET_COMPRESSION="snappy"  # Fast compression
```

### For Smaller Test Runs

For testing with smaller data volumes:

```bash
# Generate only 1 day of data (~5.7TB)
DAYS_TO_GENERATE="1"

# Or reduce data per bucket
GB_PER_BUCKET="1"  # 1GB per bucket instead of 20GB
PARTITIONS_PER_BUCKET="10"  # Fewer files per bucket
```

## S3 Costs Estimation

### Storage Costs (S3 Standard)
- 23TB stored: ~$530/month
- 1TB stored: ~$23/month

### Request Costs
- PUT requests: ~1.15M requests (100 files × 1,152 buckets × 10 tables) = ~$5.75
- GET requests during queries: Variable based on usage

### Data Transfer
- Ingress (upload): Free
- Egress (download): $0.09/GB after first 1GB

### Cost Optimization Tips
1. Use S3 Intelligent-Tiering for automatic cost optimization
2. Set lifecycle policies to delete old test data
3. Use S3 endpoints within the same region as your Spark cluster
4. Consider using S3 Glacier for long-term storage of test results

## Monitoring and Troubleshooting

### Check S3 Write Progress
Monitor the logs:
```bash
tail -f logs/generator-*.log
```

### Common Issues

#### Authentication Errors
```
Status Code: 403, AWS Service: S3
```
**Solution**: Verify your AWS credentials and bucket permissions

#### Slow Upload Speed
**Solutions**:
1. Increase `S3_MULTIPART_SIZE` and `S3_MAX_CONNECTIONS`
2. Use an EC2 instance in the same region as your S3 bucket
3. Enable S3 Transfer Acceleration on your bucket

#### Out of Memory Errors
**Solutions**:
1. Increase `DRIVER_MEMORY` and `EXECUTOR_MEMORY`
2. Reduce `PARTITIONS_PER_BUCKET`
3. Use `S3_FAST_UPLOAD_BUFFER="disk"` instead of `"array"`

#### Timeout Errors
**Solutions**:
1. Increase timeout values:
```bash
S3_CONNECTION_TIMEOUT="600000"  # 10 minutes
S3_SOCKET_TIMEOUT="600000"
S3_REQUEST_TIMEOUT="0"  # No timeout
```
2. Increase retry attempts:
```bash
S3_MAX_ERROR_RETRY="30"
```

### Verify S3 Data

List generated tables:
```bash
aws s3 ls s3://your-bucket/delta-lake-bmt/ --recursive --summarize
```

Check table size:
```bash
aws s3 ls s3://your-bucket/delta-lake-bmt/web_traffic/ --recursive --summarize | grep "Total Size"
```

## Running on AWS EMR

For production workloads, consider using AWS EMR:

```bash
# Create EMR cluster with Delta Lake
aws emr create-cluster \
  --name "Delta Lake BMT Generator" \
  --release-label emr-6.10.0 \
  --applications Name=Spark Name=Hadoop \
  --ec2-attributes KeyName=your-key \
  --instance-type m5.4xlarge \
  --instance-count 11 \
  --use-default-roles \
  --configurations file://emr-config.json
```

Example `emr-config.json`:
```json
[
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.jars.packages": "io.delta:delta-core_2.12:2.4.0",
      "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  }
]
```

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use IAM roles** instead of access keys in production
3. **Enable S3 bucket encryption** at rest
4. **Use VPC endpoints** for S3 access from EC2
5. **Enable S3 access logging** for audit trails
6. **Set up bucket policies** to restrict access
7. **Use temporary credentials** when possible
8. **Rotate access keys** regularly

## Additional Resources

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Spark S3A Committers](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html)
- [Delta Lake on S3](https://docs.delta.io/latest/delta-storage.html#amazon-s3)
- [S3 Performance Optimization](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)