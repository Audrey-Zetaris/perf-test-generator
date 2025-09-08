# Delta Lake Performance Test Generator

A high-performance data generator for creating large-scale Delta Lake datasets with realistic data patterns for benchmarking and testing.

## Features

- **Massive Scale**: Generates 20GB per 5-minute bucket with 100 part files each
- **Time-Series Data**: 4 days of data across 1,152 time buckets (23TB total)
- **Hierarchical Partitioning**: Year/Month/Day/Hour/5-minute bucket structure
- **Multiple Tables**: Web traffic, mobile traffic, user activity, and transactions
- **Optimized Queries**: 10 pre-built BMT queries with partition filtering
- **S3-Optimized**: Default configuration for AWS S3 with performance tuning

## Quick Start

### Prerequisites

- Java 8 or higher
- Apache Spark 3.5.1
- Scala 2.12.x
- 32GB+ RAM recommended for S3 generation
- AWS S3 bucket with write permissions

### Installation

1. Extract the distribution package:
```bash
unzip perf-test-generator-1.0.0.zip
cd perf-test-generator-1.0.0
```

2. Configure S3 settings:
```bash
vi conf/generator.conf
# Add your S3 bucket name and AWS credentials
```

3. Run the generator:
```bash
./bin/start-generator.sh
```

### Local Testing (Without S3)

For local filesystem testing:
```bash
cp conf/local-example.conf conf/generator.conf
./bin/start-generator.sh
```

## Configuration

### Main Configuration File: `conf/generator.conf`

The default configuration is optimized for S3 with:
- `S3_BUCKET`: Your S3 bucket name (REQUIRED)
- `AWS_ACCESS_KEY` / `AWS_SECRET_KEY`: AWS credentials
- `DRIVER_MEMORY`: 16GB (optimized for S3)
- `EXECUTOR_MEMORY`: 16GB (optimized for S3)
- `DAYS_TO_GENERATE`: 4 days (23TB total)
- `GB_PER_BUCKET`: 20GB per 5-minute bucket
- `PARTITIONS_PER_BUCKET`: 100 part files per bucket

### S3 Configuration

The configuration file is pre-configured for optimal S3 performance. Just add your bucket and credentials:

```bash
vi conf/generator.conf
# Set S3_BUCKET="your-bucket-name"
# Set AWS_ACCESS_KEY and AWS_SECRET_KEY
```

For detailed S3 options, see [S3_SETUP.md](S3_SETUP.md).

## Data Schema

### Tables Generated

1. **web_traffic**: Web browsing events
   - 20M records per 5-min bucket
   - Fields: user_id, session_id, page_url, load_time, country, etc.

2. **mobile_traffic**: Mobile app events  
   - 20M records per 5-min bucket
   - Fields: user_id, app_name, screen_name, action_type, etc.

3. **user_activity**: User interaction events
   - 20M records per 5-min bucket
   - Fields: user_id, activity_type, product_id, score, etc.

4. **transactions**: Financial transactions
   - 4M records per 5-min bucket
   - Fields: transaction_id, amount, payment_method, status, etc.

### Partitioning Structure

All tables are partitioned by:
- `year` (INT)
- `month` (INT)
- `day` (INT)
- `hour` (INT)
- `minute_bucket` (INT: 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)

Example path:
```
/base_path/web_traffic/year=2025/month=9/day=8/hour=14/minute_bucket=30/
```

## Benchmark Queries

The application includes 10 optimized queries:

1. **Cross-Platform User Activity**: Joins web, mobile, and activity data
2. **High-Value Customer Analysis**: Transaction analysis with user profiles
3. **Session Journey Analysis**: Traces user sessions across platforms
4. **Geographic Analysis**: User behavior by geography
5. **Device Fraud Analysis**: Suspicious device pattern detection
6. **Conversion Funnel**: User journey from traffic to transaction
7. **Real-Time Anomaly Detection**: Statistical anomaly detection
8. **User Segmentation**: Behavioral user clustering
9. **Revenue Attribution**: Multi-touch attribution analysis
10. **Performance Analysis**: Cross-platform performance metrics

All queries include partition filters for optimal performance.

## Running on a Cluster

### YARN Mode
```bash
SPARK_MASTER=yarn ./bin/start-generator.sh
```

### Standalone Cluster
```bash
SPARK_MASTER=spark://master:7077 ./bin/start-generator.sh
```

### Kubernetes
```bash
SPARK_MASTER=k8s://https://k8s-master:443 ./bin/start-generator.sh
```

## Performance Tuning

### Memory Settings
For large-scale generation, increase memory:
```bash
export DRIVER_MEMORY=16g
export EXECUTOR_MEMORY=16g
./bin/start-generator.sh
```

### Parallelism
Adjust parallelism based on cluster size:
```bash
export SPARK_SHUFFLE_PARTITIONS=400
export SPARK_DEFAULT_PARALLELISM=400
./bin/start-generator.sh
```

## Directory Structure

```
perf-test-generator-1.0.0/
├── bin/
│   └── start-generator.sh      # Main startup script
├── conf/
│   ├── generator.conf          # Main configuration
│   ├── application.conf        # Application settings
│   └── log4j.properties        # Logging configuration
├── lib/
│   └── perf-test-generator-1.0.0.jar  # Application JAR
└── logs/                       # Log files (created at runtime)
```

## Monitoring

Logs are written to:
- Console output
- `logs/generator-YYYYMMDD-HHMMSS.log`

Monitor progress through:
- Record counts per table
- Data size estimates
- Time per bucket
- Query execution times

## Troubleshooting

### Out of Memory
Increase driver/executor memory in `conf/generator.conf`

### Slow Performance
- Increase `PARTITIONS_PER_BUCKET` for better parallelism
- Ensure sufficient cluster resources
- Check network bandwidth for S3 writes

### S3 Write Issues
- Verify AWS credentials
- Check S3 bucket permissions
- Ensure S3 endpoint is correct

## License

Copyright (c) 2025 Zetaris

## Support

For issues or questions, please contact the development team.