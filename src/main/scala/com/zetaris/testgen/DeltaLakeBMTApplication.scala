package com.zetaris.testgen

import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.util.Random

object DeltaLakeBMTApplication {

  case class Config(
                     basePath: String = "/tmp/delta-lake-bmt",
                     partitionInterval: Int = 5, // minutes
                     daysToGenerate: Int = 1, // Generate 1 day of data
                     targetDataSizeGBPerBucket: Double = 20.0, // 20GB per 5-min bucket
                     partitionsPerBucket: Int = 100, // 100 part files per bucket
                     recordsPerBucket: Long = 20000000L // ~20M records for 20GB (assuming ~1KB per record)
                   )

  def parseCommandLineArgs(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("perf-test-generator") {
      head("Delta Lake Performance Test Generator", "1.0.0")
      
      opt[String]("base-path")
        .action((x, c) => c.copy(basePath = x))
        .text("Base path for Delta Lake tables")
        
      opt[Int]("days")
        .action((x, c) => c.copy(daysToGenerate = x))
        .text("Number of days of data to generate")
        
      opt[Double]("gb-per-bucket")
        .action((x, c) => c.copy(targetDataSizeGBPerBucket = x))
        .text("Target data size in GB per 5-minute bucket")
        
      opt[Int]("partitions-per-bucket")
        .action((x, c) => c.copy(partitionsPerBucket = x))
        .text("Number of partitions per bucket")
        
      opt[Long]("records-per-bucket")
        .action((x, c) => c.copy(recordsPerBucket = x))
        .text("Number of records per bucket")
    }
    
    parser.parse(args, Config()) match {
      case Some(config) => config
      case None =>
        sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    val config = parseCommandLineArgs(args)

    val spark = SparkSession.builder()
      .appName("DeltaLake BMT Application")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .getOrCreate()

    val generator = new DataGenerator(spark, config)
    val queryEngine = new QueryEngine(spark, config)

    try {
      println("=== Starting Delta Lake BMT Data Generation ===")

      // Generate fact tables for days of data
      // 1 day * 288 buckets per day = 288 buckets (5-minute intervals)
      val bucketsPerDay = 288 // 288 five-minute buckets per day (24 hours * 12 buckets/hour)
      val totalBuckets = config.daysToGenerate * bucketsPerDay
      
      println(s"Generating ${config.daysToGenerate} days of data (${totalBuckets} 5-minute buckets)")
      println(s"Target: ${config.targetDataSizeGBPerBucket}GB per bucket with ${config.partitionsPerBucket} part files")
      println(s"Total data size: ${totalBuckets * config.targetDataSizeGBPerBucket}GB")
      
      val buckets = generateTimeBuckets(totalBuckets)

      buckets.zipWithIndex.foreach { case (bucket, index) =>
        println(s"\n[${index + 1}/${totalBuckets}] Generating data for bucket: ${bucket.timestamp_str}")
        println(s"  Year=${bucket.year}, Month=${bucket.month}, Day=${bucket.day}, Hour=${bucket.hour}, Minute=${bucket.minute_bucket}")
        
        val startTime = System.currentTimeMillis()
        generator.generateWebTrafficData(bucket)
        generator.generateMobileTrafficData(bucket)
        generator.generateUserActivityData(bucket)
        generator.generateTransactionData(bucket)
        
        val elapsedTime = (System.currentTimeMillis() - startTime) / 1000.0
        println(f"  Bucket completed in $elapsedTime%.2f seconds")
      }

      println("=== Data Generation Complete ===")
      println("=== Running Sample BMT Queries ===")

      // Run sample queries
      queryEngine.runSampleQueries()

    } finally {
      spark.stop()
    }
  }

  case class TimeBucket(
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    minute_bucket: Int,  // 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55
    timestamp_str: String
  )

  def generateTimeBuckets(count: Int): List[TimeBucket] = {
    val now = LocalDateTime.now()
    (0 until count).map { i =>
      val bucketTime = now.minusMinutes(i * 5)
      val minuteBucket = (bucketTime.getMinute / 5) * 5  // Round down to nearest 5-minute interval
      
      TimeBucket(
        year = bucketTime.getYear,
        month = bucketTime.getMonthValue,
        day = bucketTime.getDayOfMonth,
        hour = bucketTime.getHour,
        minute_bucket = minuteBucket,
        timestamp_str = bucketTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      )
    }.toList
  }
}

class DataGenerator(spark: SparkSession, config: DeltaLakeBMTApplication.Config) extends Serializable {
  import spark.implicits._

  // Generate IDs on-demand instead of pre-creating arrays
  private def generateUserId(): String = s"user_${scala.util.Random.nextInt(100000) + 1}"
  private def generateSessionId(): String = UUID.randomUUID().toString
  private def generateDeviceId(): String = s"device_${scala.util.Random.nextInt(50000) + 1}"
  private def generateIpAddress(): String = s"${scala.util.Random.nextInt(255)}.${scala.util.Random.nextInt(255)}.${scala.util.Random.nextInt(255)}.${scala.util.Random.nextInt(255)}"
  
  private val countries = Array("US", "UK", "DE", "FR", "JP", "IN", "BR", "CA", "AU", "RU")
  private val cities = Array("New York", "London", "Berlin", "Paris", "Tokyo", "Mumbai", "São Paulo", "Toronto", "Sydney", "Moscow")
  private val browsers = Array("Chrome", "Firefox", "Safari", "Edge", "Opera")
  private val osTypes = Array("Windows", "MacOS", "Linux", "iOS", "Android")
  private val deviceTypes = Array("Mobile", "Desktop", "Tablet")

  def generateWebTrafficData(timeBucket: DeltaLakeBMTApplication.TimeBucket): Unit = {
    // println(s"Generating web traffic data for ${timeBucket.timestamp_str}")

    val webTrafficDF = generateLargeDataFrame(config.recordsPerBucket, config.partitionsPerBucket) { index =>
      val timestamp = generateTimestampInBucket(timeBucket)
      WebTrafficRecord(
        event_id = s"web_${UUID.randomUUID()}",
        user_id = generateUserId(),
        session_id = generateSessionId(),
        device_id = generateDeviceId(),
        timestamp = timestamp,
        page_url = s"https://example.com/page${scala.util.Random.nextInt(10000)}",
        referrer = if (scala.util.Random.nextDouble() > 0.3) s"https://referrer${scala.util.Random.nextInt(100)}.com" else null,
        ip_address = generateIpAddress(),
        user_agent = s"${browsers(scala.util.Random.nextInt(browsers.length))}/1.0",
        country = countries(scala.util.Random.nextInt(countries.length)),
        city = cities(scala.util.Random.nextInt(cities.length)),
        page_load_time = scala.util.Random.nextInt(5000) + 100,
        bounce_rate = scala.util.Random.nextDouble(),
        conversion = scala.util.Random.nextBoolean(),
        revenue = if (scala.util.Random.nextDouble() > 0.95) scala.util.Random.nextDouble() * 1000 else 0.0,
        browser = browsers(scala.util.Random.nextInt(browsers.length)),
        os = osTypes(scala.util.Random.nextInt(osTypes.length)),
        device_type = deviceTypes(scala.util.Random.nextInt(deviceTypes.length)),
        year = timeBucket.year,
        month = timeBucket.month,
        day = timeBucket.day,
        hour = timeBucket.hour,
        minute_bucket = timeBucket.minute_bucket
      )
    }

    writeToDeltalake(webTrafficDF, "web_traffic", timeBucket.timestamp_str)
  }

  def generateMobileTrafficData(timeBucket: DeltaLakeBMTApplication.TimeBucket): Unit = {
    // println(s"Generating mobile traffic data for ${timeBucket.timestamp_str}")

    val mobileTrafficDF = generateLargeDataFrame(config.recordsPerBucket, config.partitionsPerBucket) { index =>
      val timestamp = generateTimestampInBucket(timeBucket)
      val userId = generateUserId()
      val sessionId = generateSessionId()
      val deviceId = generateDeviceId()

      MobileTrafficRecord(
        event_id = s"mobile_${UUID.randomUUID()}",
        user_id = userId,
        session_id = sessionId,
        device_id = deviceId,
        timestamp = timestamp,
        app_name = s"MobileApp${scala.util.Random.nextInt(50)}",
        app_version = s"${scala.util.Random.nextInt(5)}.${scala.util.Random.nextInt(10)}.${scala.util.Random.nextInt(10)}",
        screen_name = s"Screen${scala.util.Random.nextInt(100)}",
        action_type = Array("click", "swipe", "scroll", "tap", "long_press").apply(scala.util.Random.nextInt(5)),
        ip_address = generateIpAddress(),
        country = countries(scala.util.Random.nextInt(countries.length)),
        city = cities(scala.util.Random.nextInt(cities.length)),
        network_type = Array("wifi", "4g", "5g", "3g").apply(scala.util.Random.nextInt(4)),
        battery_level = scala.util.Random.nextInt(100),
        memory_usage = scala.util.Random.nextInt(8192),
        cpu_usage = scala.util.Random.nextDouble() * 100,
        crash_occurred = scala.util.Random.nextDouble() > 0.99,
        session_duration = scala.util.Random.nextInt(3600000), // milliseconds
        data_usage_mb = scala.util.Random.nextDouble() * 100,
        year = timeBucket.year,
        month = timeBucket.month,
        day = timeBucket.day,
        hour = timeBucket.hour,
        minute_bucket = timeBucket.minute_bucket
      )
    }

    writeToDeltalake(mobileTrafficDF, "mobile_traffic", timeBucket.timestamp_str)
  }

  def generateUserActivityData(timeBucket: DeltaLakeBMTApplication.TimeBucket): Unit = {
    // println(s"Generating user activity data for ${timeBucket.timestamp_str}")

    val userActivityDF = generateLargeDataFrame(config.recordsPerBucket, config.partitionsPerBucket) { index =>
      val timestamp = generateTimestampInBucket(timeBucket)
      UserActivityRecord(
        activity_id = s"activity_${UUID.randomUUID()}",
        user_id = generateUserId(),
        session_id = generateSessionId(),
        device_id = generateDeviceId(),
        timestamp = timestamp,
        activity_type = Array("login", "logout", "purchase", "view", "search", "share", "like", "comment").apply(scala.util.Random.nextInt(8)),
        duration_seconds = scala.util.Random.nextInt(7200),
        ip_address = generateIpAddress(),
        country = countries(scala.util.Random.nextInt(countries.length)),
        city = cities(scala.util.Random.nextInt(cities.length)),
        product_id = if (scala.util.Random.nextDouble() > 0.5) s"product_${scala.util.Random.nextInt(100000)}" else null,
        category_id = s"category_${scala.util.Random.nextInt(1000)}",
        subcategory_id = s"subcategory_${scala.util.Random.nextInt(5000)}",
        tags = Array.fill(scala.util.Random.nextInt(5) + 1)(s"tag${scala.util.Random.nextInt(1000)}").mkString(","),
        score = scala.util.Random.nextInt(100),
        is_premium_user = scala.util.Random.nextBoolean(),
        year = timeBucket.year,
        month = timeBucket.month,
        day = timeBucket.day,
        hour = timeBucket.hour,
        minute_bucket = timeBucket.minute_bucket
      )
    }

    writeToDeltalake(userActivityDF, "user_activity", timeBucket.timestamp_str)
  }

  def generateTransactionData(timeBucket: DeltaLakeBMTApplication.TimeBucket): Unit = {
    // println(s"Generating transaction data for ${timeBucket.timestamp_str}")

    val transactionDF = generateLargeDataFrame(config.recordsPerBucket / 5, config.partitionsPerBucket) { index => // Fewer transactions
      val timestamp = generateTimestampInBucket(timeBucket)
      TransactionRecord(
        transaction_id = s"txn_${UUID.randomUUID()}",
        user_id = generateUserId(),
        session_id = generateSessionId(),
        device_id = generateDeviceId(),
        timestamp = timestamp,
        amount = scala.util.Random.nextDouble() * 2000 + 10, // $10 - $2010
        currency = Array("USD", "EUR", "GBP", "JPY", "INR").apply(scala.util.Random.nextInt(5)),
        payment_method = Array("credit_card", "debit_card", "paypal", "apple_pay", "google_pay").apply(scala.util.Random.nextInt(5)),
        merchant_id = s"merchant_${scala.util.Random.nextInt(10000)}",
        product_ids = Array.fill(scala.util.Random.nextInt(5) + 1)(s"product_${scala.util.Random.nextInt(100000)}").mkString(","),
        ip_address = generateIpAddress(),
        country = countries(scala.util.Random.nextInt(countries.length)),
        city = cities(scala.util.Random.nextInt(cities.length)),
        is_fraud = scala.util.Random.nextDouble() > 0.98,
        risk_score = scala.util.Random.nextInt(100),
        processing_time_ms = scala.util.Random.nextInt(5000) + 100,
        status = Array("completed", "pending", "failed", "cancelled").apply(scala.util.Random.nextInt(4)),
        year = timeBucket.year,
        month = timeBucket.month,
        day = timeBucket.day,
        hour = timeBucket.hour,
        minute_bucket = timeBucket.minute_bucket
      )
    }

    writeToDeltalake(transactionDF, "transactions", timeBucket.timestamp_str)
  }

  private def generateLargeDataFrame[T: Encoder](numRecords: Long, numPartitions: Int = 100)(recordGenerator: Long => T): DataFrame = {
    // Force exactly numPartitions partitions for consistent file output
    spark.range(0, numRecords, 1, numPartitions)
      .map(l => recordGenerator(l.toLong))
      .toDF()
      .repartition(numPartitions) // Ensure exactly numPartitions output files
  }

  private def generateTimestampInBucket(bucket: DeltaLakeBMTApplication.TimeBucket): Long = {
    val bucketStart = LocalDateTime.of(bucket.year, bucket.month, bucket.day, bucket.hour, bucket.minute_bucket, 0)
    val randomOffset = scala.util.Random.nextInt(5 * 60) // Random seconds within 5 minutes
    bucketStart.plusSeconds(randomOffset).toEpochSecond(ZoneOffset.UTC) * 1000
  }


  private def writeToDeltalake(df: DataFrame, tableName: String, bucket: String): Unit = {
    val path = s"${config.basePath}/$tableName"
    
    df.write
      .format("delta")
      .mode("append")
      .partitionBy("year", "month", "day", "hour", "minute_bucket")
      .option("mergeSchema", "true")
      .option("dataChange", "true")
      .save(path)

    // Log expected count (no actual computation)
    val expectedRecords = if (tableName == "transactions") config.recordsPerBucket / 5 else config.recordsPerBucket
    val estimatedSizeGB = (expectedRecords * 268) / (1024.0 * 1024.0 * 1024.0) // Using actual ~268 bytes per record
    println(f"  ✓ Written $expectedRecords%,d records (~$estimatedSizeGB%.2fGB) to $tableName")
  }
}

class QueryEngine(spark: SparkSession, config: DeltaLakeBMTApplication.Config) {
  
  // Helper to generate partition filter for the last N hours
  private def getPartitionFilter(hoursBack: Int = 24, tableAlias: String = ""): String = {
    val now = java.time.LocalDateTime.now()
    val startTime = now.minusHours(hoursBack)
    val prefix = if (tableAlias.nonEmpty) s"$tableAlias." else ""
    
    s"""${prefix}year = ${now.getYear} 
       |AND ${prefix}month = ${now.getMonthValue} 
       |AND ${prefix}day >= ${startTime.getDayOfMonth}
       |AND ${prefix}day <= ${now.getDayOfMonth}
       |AND (${prefix}day > ${startTime.getDayOfMonth} OR ${prefix}hour >= ${startTime.getHour})
       |AND (${prefix}day < ${now.getDayOfMonth} OR ${prefix}hour <= ${now.getHour})""".stripMargin.replaceAll("\n", " ")
  }
  
  // Helper to get the most recent data filter
  private def getRecentDataFilter(tableAlias: String = ""): String = {
    getPartitionFilter(1, tableAlias) // Last 1 hour of data
  }

  def runSampleQueries(): Unit = {
    // Register Delta tables as views
    registerDeltaTables()

    val queries = List(
      query1_CrossPlatformUserActivity,
      query2_HighValueCustomerAnalysis,
      query3_SessionJourneyAnalysis,
      query4_GeographicCrossChannelAnalysis,
      query5_DeviceFraudAnalysis,
      query6_ConversionFunnelAnalysis,
      query7_RealTimeAnomalyDetection,
      query8_UserSegmentationAnalysis,
      query9_RevenueAttributionAnalysis,
      query10_CrossPlatformPerformanceAnalysis
    )

    queries.zipWithIndex.foreach { case (query, index) =>
      println(s"\n=== Executing Query ${index + 1} ===")
      println(query.description)
      executeTimedQuery(query.sql, s"Query_${index + 1}")
    }
  }

  private def registerDeltaTables(): Unit = {
    val tables = List("web_traffic", "mobile_traffic", "user_activity", "transactions")

    tables.foreach { tableName =>
      val path = s"${config.basePath}/$tableName"
      spark.read.format("delta").load(path).createOrReplaceTempView(tableName)
      println(s"Registered Delta table: $tableName")
    }
  }

  private def executeTimedQuery(sql: String, queryName: String): Unit = {
    val startTime = System.currentTimeMillis()
    try {
      val result = spark.sql(sql)
      val count = result.count()
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime

      println(s"$queryName completed in ${duration}ms, returned $count rows")
      result.show(20, truncate = false)
    } catch {
      case e: Exception =>
        println(s"$queryName failed: ${e.getMessage}")
    }
  }

  // Query definitions
  case class BMTQuery(description: String, sql: String)

  def query1_CrossPlatformUserActivity = {
    val partitionFilter = getPartitionFilter(24) // Last 24 hours
    BMTQuery(
      "Cross-Platform User Activity Analysis - Join web and mobile traffic with user activities",
      s"""
        |SELECT
        |  ua.user_id,
        |  COUNT(DISTINCT wt.session_id) as web_sessions,
        |  COUNT(DISTINCT mt.session_id) as mobile_sessions,
        |  COUNT(DISTINCT ua.activity_id) as total_activities,
        |  AVG(wt.page_load_time) as avg_web_load_time,
        |  AVG(mt.session_duration) as avg_mobile_session_duration,
        |  SUM(CASE WHEN ua.activity_type = 'purchase' THEN 1 ELSE 0 END) as purchases
        |FROM user_activity ua
        |LEFT JOIN web_traffic wt ON 
        |  ua.user_id = wt.user_id 
        |  AND ua.session_id = wt.session_id
        |  AND wt.year = ua.year AND wt.month = ua.month 
        |  AND wt.day = ua.day AND wt.hour = ua.hour
        |LEFT JOIN mobile_traffic mt ON 
        |  ua.user_id = mt.user_id 
        |  AND ua.session_id = mt.session_id
        |  AND mt.year = ua.year AND mt.month = ua.month 
        |  AND mt.day = ua.day AND mt.hour = ua.hour
        |WHERE ${partitionFilter.replaceAll("(?m)^\\s+", "").replaceAll("\n", " ").replaceAll("  +", " ").replace("year", "ua.year").replace("month", "ua.month").replace("day", "ua.day").replace("hour", "ua.hour")}
        |GROUP BY ua.user_id
        |HAVING total_activities > 5
        |ORDER BY total_activities DESC
        |LIMIT 100
      """.stripMargin
    )
  }

  def query2_HighValueCustomerAnalysis = {
    val partitionFilter = getPartitionFilter(48) // Last 48 hours for transaction analysis
    BMTQuery(
      "High-Value Customer Analysis - Join transactions with user activity and web traffic",
      s"""
        |SELECT
        |  t.user_id,
        |  COUNT(DISTINCT t.transaction_id) as transaction_count,
        |  SUM(t.amount) as total_spent,
        |  AVG(t.amount) as avg_transaction_value,
        |  COUNT(DISTINCT wt.session_id) as web_sessions,
        |  COUNT(DISTINCT ua.activity_id) as activities,
        |  wt.country,
        |  wt.device_type
        |FROM transactions t
        |JOIN user_activity ua ON 
        |  t.user_id = ua.user_id AND t.session_id = ua.session_id
        |  AND ua.year = t.year AND ua.month = t.month 
        |  AND ua.day = t.day AND ua.hour = t.hour
        |JOIN web_traffic wt ON 
        |  t.user_id = wt.user_id AND t.session_id = wt.session_id
        |  AND wt.year = t.year AND wt.month = t.month 
        |  AND wt.day = t.day AND wt.hour = t.hour
        |WHERE t.status = 'completed' AND t.is_fraud = false
        |  AND ${partitionFilter.replace("year", "t.year").replace("month", "t.month").replace("day", "t.day").replace("hour", "t.hour")}
        |GROUP BY t.user_id, wt.country, wt.device_type
        |HAVING total_spent > 1000
        |ORDER BY total_spent DESC
        |LIMIT 50
      """.stripMargin
    )
  }

  def query3_SessionJourneyAnalysis = {
    val partitionFilter = getPartitionFilter(6) // Last 6 hours for session analysis
    BMTQuery(
      "Session Journey Analysis - Trace user sessions across all platforms",
      s"""
        |WITH session_events AS (
        |  SELECT session_id, user_id, 'web' as platform, timestamp, page_url as event_detail, device_id,
        |         year, month, day, hour
        |  FROM web_traffic
        |  WHERE ${partitionFilter}
        |  UNION ALL
        |  SELECT session_id, user_id, 'mobile' as platform, timestamp, screen_name as event_detail, device_id,
        |         year, month, day, hour
        |  FROM mobile_traffic
        |  WHERE ${partitionFilter}
        |  UNION ALL
        |  SELECT session_id, user_id, 'activity' as platform, timestamp, activity_type as event_detail, device_id,
        |         year, month, day, hour
        |  FROM user_activity
        |  WHERE ${partitionFilter}
        |)
      |SELECT
      |  se.session_id,
      |  se.user_id,
      |  COUNT(*) as total_events,
      |  COUNT(DISTINCT se.platform) as platforms_used,
      |  MIN(se.timestamp) as session_start,
      |  MAX(se.timestamp) as session_end,
      |  (MAX(se.timestamp) - MIN(se.timestamp)) / 1000 as session_duration_seconds,
      |  COUNT(DISTINCT se.device_id) as devices_used,
      |  COALESCE(SUM(t.amount), 0) as session_revenue
      |FROM session_events se
      |LEFT JOIN transactions t ON se.session_id = t.session_id AND se.user_id = t.user_id
      |GROUP BY se.session_id, se.user_id
      |HAVING platforms_used > 1 AND session_duration_seconds > 300
      |ORDER BY session_revenue DESC, session_duration_seconds DESC
      |LIMIT 100
      """.stripMargin
    )
  }

  def query4_GeographicCrossChannelAnalysis = {
    val partitionFilter = getPartitionFilter(24)
    BMTQuery(
    "Geographic Cross-Channel Analysis - Analyze user behavior by geography across channels",
    """
      |SELECT
      |  COALESCE(wt.country, mt.country, ua.country) as country,
      |  COALESCE(wt.city, mt.city, ua.city) as city,
      |  COUNT(DISTINCT COALESCE(wt.user_id, mt.user_id, ua.user_id)) as unique_users,
      |  COUNT(DISTINCT wt.session_id) as web_sessions,
      |  COUNT(DISTINCT mt.session_id) as mobile_sessions,
      |  COUNT(DISTINCT ua.activity_id) as user_activities,
      |  AVG(wt.page_load_time) as avg_web_load_time,
      |  AVG(mt.cpu_usage) as avg_mobile_cpu_usage,
      |  SUM(CASE WHEN ua.activity_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
      |  COALESCE(SUM(t.amount), 0) as total_revenue
      |FROM web_traffic wt
      |FULL OUTER JOIN mobile_traffic mt ON wt.user_id = mt.user_id AND wt.session_id = mt.session_id
      |FULL OUTER JOIN user_activity ua ON COALESCE(wt.user_id, mt.user_id) = ua.user_id
      |  AND COALESCE(wt.session_id, mt.session_id) = ua.session_id
      |LEFT JOIN transactions t ON COALESCE(wt.user_id, mt.user_id, ua.user_id) = t.user_id
      |WHERE COALESCE(wt.country, mt.country, ua.country) IS NOT NULL
      |GROUP BY country, city
      |HAVING unique_users > 10
      |ORDER BY total_revenue DESC, unique_users DESC
      |LIMIT 50
    """.stripMargin
    )
  }

  val query5_DeviceFraudAnalysis = BMTQuery(
    "Device-Based Fraud Analysis - Identify suspicious device patterns across platforms",
    """
      |SELECT
      |  COALESCE(wt.device_id, mt.device_id, ua.device_id, t.device_id) as device_id,
      |  COUNT(DISTINCT COALESCE(wt.user_id, mt.user_id, ua.user_id, t.user_id)) as unique_users,
      |  COUNT(DISTINCT COALESCE(wt.ip_address, mt.ip_address, ua.ip_address, t.ip_address)) as unique_ips,
      |  COUNT(DISTINCT t.transaction_id) as transactions,
      |  SUM(CASE WHEN t.is_fraud = true THEN 1 ELSE 0 END) as fraud_transactions,
      |  AVG(t.risk_score) as avg_risk_score,
      |  SUM(t.amount) as total_transaction_amount,
      |  COUNT(DISTINCT wt.session_id) as web_sessions,
      |  COUNT(DISTINCT mt.session_id) as mobile_sessions,
      |  COALESCE(wt.device_type, 'mobile') as device_type
      |FROM web_traffic wt
      |FULL OUTER JOIN mobile_traffic mt ON wt.device_id = mt.device_id
      |FULL OUTER JOIN user_activity ua ON COALESCE(wt.device_id, mt.device_id) = ua.device_id
      |FULL OUTER JOIN transactions t ON COALESCE(wt.device_id, mt.device_id, ua.device_id) = t.device_id
      |WHERE COALESCE(wt.device_id, mt.device_id, ua.device_id, t.device_id) IS NOT NULL
      |GROUP BY device_id, device_type
      |HAVING unique_users > 5 OR fraud_transactions > 0 OR avg_risk_score > 70
      |ORDER BY fraud_transactions DESC, avg_risk_score DESC, unique_users DESC
      |LIMIT 100
    """.stripMargin
  )

  val query6_ConversionFunnelAnalysis = BMTQuery(
    "Conversion Funnel Analysis - Track user journey from traffic to transaction",
    """
      |WITH user_funnel AS (
      |  SELECT
      |    u.user_id,
      |    MAX(CASE WHEN wt.user_id IS NOT NULL THEN 1 ELSE 0 END) as had_web_traffic,
      |    MAX(CASE WHEN mt.user_id IS NOT NULL THEN 1 ELSE 0 END) as had_mobile_traffic,
      |    MAX(CASE WHEN ua.activity_type = 'view' THEN 1 ELSE 0 END) as had_view,
      |    MAX(CASE WHEN ua.activity_type = 'search' THEN 1 ELSE 0 END) as had_search,
      |    MAX(CASE WHEN ua.activity_type = 'purchase' THEN 1 ELSE 0 END) as had_purchase_activity,
      |    MAX(CASE WHEN t.user_id IS NOT NULL AND t.status = 'completed' THEN 1 ELSE 0 END) as had_transaction,
      |    COUNT(DISTINCT ua.session_id) as total_sessions,
      |    AVG(wt.page_load_time) as avg_load_time
      |  FROM (SELECT DISTINCT user_id FROM user_activity) u
      |  LEFT JOIN web_traffic wt ON u.user_id = wt.user_id
      |  LEFT JOIN mobile_traffic mt ON u.user_id = mt.user_id
      |  LEFT JOIN user_activity ua ON u.user_id = ua.user_id
      |  LEFT JOIN transactions t ON u.user_id = t.user_id
      |  GROUP BY u.user_id
      |)
      |SELECT
      |  'Traffic' as funnel_step,
      |  SUM(had_web_traffic + had_mobile_traffic) as users_count,
      |  ROUND(100.0 * SUM(had_web_traffic + had_mobile_traffic) / COUNT(*), 2) as conversion_rate
      |FROM user_funnel
      |UNION ALL
      |SELECT
      |  'View' as funnel_step,
      |  SUM(had_view) as users_count,
      |  ROUND(100.0 * SUM(had_view) / NULLIF(SUM(had_web_traffic + had_mobile_traffic), 0), 2) as conversion_rate
      |FROM user_funnel
      |UNION ALL
      |SELECT
      |  'Search' as funnel_step,
      |  SUM(had_search) as users_count,
      |  ROUND(100.0 * SUM(had_search) / NULLIF(SUM(had_view), 0), 2) as conversion_rate
      |FROM user_funnel
      |UNION ALL
      |SELECT
      |  'Purchase Activity' as funnel_step,
      |  SUM(had_purchase_activity) as users_count,
      |  ROUND(100.0 * SUM(had_purchase_activity) / NULLIF(SUM(had_search), 0), 2) as conversion_rate
      |FROM user_funnel
      |UNION ALL
      |SELECT
      |  'Completed Transaction' as funnel_step,
      |  SUM(had_transaction) as users_count,
      |  ROUND(100.0 * SUM(had_transaction) / NULLIF(SUM(had_purchase_activity), 0), 2) as conversion_rate
      |FROM user_funnel
    """.stripMargin
  )

  def query7_RealTimeAnomalyDetection = {
    val partitionFilter = getPartitionFilter(12) // Last 12 hours
    BMTQuery(
      "Real-Time Anomaly Detection - Identify unusual patterns across platforms",
      s"""
        |WITH platform_metrics AS (
        |  SELECT
        |    year, month, day, hour, minute_bucket,
        |    'web' as platform,
        |    COUNT(*) as event_count,
        |    COUNT(DISTINCT user_id) as unique_users,
        |    AVG(page_load_time) as avg_metric,
        |    COUNT(DISTINCT ip_address) as unique_ips
        |  FROM web_traffic
        |  WHERE ${partitionFilter}
        |  GROUP BY year, month, day, hour, minute_bucket
        |  UNION ALL
        |  SELECT
        |    year, month, day, hour, minute_bucket,
        |    'mobile' as platform,
        |    COUNT(*) as event_count,
        |    COUNT(DISTINCT user_id) as unique_users,
        |    AVG(session_duration) as avg_metric,
        |    COUNT(DISTINCT ip_address) as unique_ips
        |  FROM mobile_traffic
        |  WHERE ${partitionFilter}
        |  GROUP BY year, month, day, hour, minute_bucket
        |),
      |platform_stats AS (
      |  SELECT
      |    platform,
      |    AVG(event_count) as avg_events,
      |    STDDEV(event_count) as stddev_events,
      |    AVG(unique_users) as avg_users,
      |    STDDEV(unique_users) as stddev_users
      |  FROM platform_metrics
      |  GROUP BY platform
      |)
        |SELECT
        |  CONCAT(pm.year, '-', pm.month, '-', pm.day, ' ', pm.hour, ':', pm.minute_bucket) as time_bucket,
        |  pm.platform,
        |  pm.event_count,
        |  pm.unique_users,
        |  pm.avg_metric,
        |  ROUND((pm.event_count - ps.avg_events) / NULLIF(ps.stddev_events, 0), 2) as event_zscore,
        |  ROUND((pm.unique_users - ps.avg_users) / NULLIF(ps.stddev_users, 0), 2) as user_zscore,
        |  CASE
        |    WHEN ABS((pm.event_count - ps.avg_events) / NULLIF(ps.stddev_events, 0)) > 2
        |         OR ABS((pm.unique_users - ps.avg_users) / NULLIF(ps.stddev_users, 0)) > 2
        |    THEN 'ANOMALY'
        |    ELSE 'NORMAL'
        |  END as anomaly_status
        |FROM platform_metrics pm
        |JOIN platform_stats ps ON pm.platform = ps.platform
        |ORDER BY ABS(event_zscore) DESC, ABS(user_zscore) DESC
        |LIMIT 50
      """.stripMargin
    )
  }

  val query8_UserSegmentationAnalysis = BMTQuery(
    "User Segmentation Analysis - Segment users based on cross-platform behavior",
    """
      |WITH user_segments AS (
      |  SELECT
      |    u.user_id,
      |    COUNT(DISTINCT wt.session_id) as web_sessions,
      |    COUNT(DISTINCT mt.session_id) as mobile_sessions,
      |    COUNT(DISTINCT ua.activity_id) as total_activities,
      |    COUNT(DISTINCT t.transaction_id) as transactions,
      |    COALESCE(SUM(t.amount), 0) as total_spent,
      |    MAX(ua.is_premium_user) as is_premium,
      |    AVG(wt.page_load_time) as avg_web_load_time,
      |    AVG(mt.session_duration) as avg_mobile_duration,
      |    COUNT(DISTINCT COALESCE(wt.country, mt.country, ua.country)) as countries_visited
      |  FROM (SELECT DISTINCT user_id FROM user_activity) u
      |  LEFT JOIN web_traffic wt ON u.user_id = wt.user_id
      |  LEFT JOIN mobile_traffic mt ON u.user_id = mt.user_id
      |  LEFT JOIN user_activity ua ON u.user_id = ua.user_id
      |  LEFT JOIN transactions t ON u.user_id = t.user_id AND t.status = 'completed'
      |  GROUP BY u.user_id
      |),
      |user_segments_classified AS (
      |  SELECT *,
      |    CASE
      |      WHEN total_spent > 500 AND transactions > 5 THEN 'High Value'
      |      WHEN total_spent > 100 AND transactions > 2 THEN 'Medium Value'
      |      WHEN total_spent > 0 OR transactions > 0 THEN 'Low Value'
      |      ELSE 'Non-Transacting'
      |    END as spending_segment,
      |    CASE
      |      WHEN web_sessions > 0 AND mobile_sessions > 0 THEN 'Cross-Platform'
      |      WHEN web_sessions > 0 THEN 'Web Only'
      |      WHEN mobile_sessions > 0 THEN 'Mobile Only'
      |      ELSE 'Activity Only'
      |    END as platform_segment
      |  FROM user_segments
      |)
      |SELECT
      |  spending_segment,
      |  platform_segment,
      |  COUNT(*) as user_count,
      |  AVG(total_spent) as avg_spent,
      |  AVG(total_activities) as avg_activities,
      |  AVG(web_sessions + mobile_sessions) as avg_sessions,
      |  SUM(CASE WHEN is_premium = true THEN 1 ELSE 0 END) as premium_users,
      |  AVG(countries_visited) as avg_countries
      |FROM user_segments_classified
      |GROUP BY spending_segment, platform_segment
      |ORDER BY user_count DESC
    """.stripMargin
  )

  val query9_RevenueAttributionAnalysis = BMTQuery(
    "Revenue Attribution Analysis - Attribute revenue to different touchpoints",
    """
      |WITH user_touchpoints AS (
      |  SELECT
      |    t.user_id,
      |    t.transaction_id,
      |    t.amount,
      |    t.timestamp as transaction_time,
      |    COUNT(DISTINCT wt.session_id) as web_sessions_before,
      |    COUNT(DISTINCT mt.session_id) as mobile_sessions_before,
      |    COUNT(DISTINCT ua.activity_id) as activities_before,
      |    MIN(wt.timestamp) as first_web_touch,
      |    MIN(mt.timestamp) as first_mobile_touch,
      |    MIN(ua.timestamp) as first_activity_touch,
      |    MAX(wt.timestamp) as last_web_touch,
      |    MAX(mt.timestamp) as last_mobile_touch,
      |    MAX(ua.timestamp) as last_activity_touch
      |  FROM transactions t
      |  LEFT JOIN web_traffic wt ON t.user_id = wt.user_id AND wt.timestamp < t.timestamp
      |  LEFT JOIN mobile_traffic mt ON t.user_id = mt.user_id AND mt.timestamp < t.timestamp
      |  LEFT JOIN user_activity ua ON t.user_id = ua.user_id AND ua.timestamp < t.timestamp
      |  WHERE t.status = 'completed' AND t.is_fraud = false
      |  GROUP BY t.user_id, t.transaction_id, t.amount, t.timestamp
      |),
      |attribution_model AS (
      |  SELECT *,
      |    CASE
      |      WHEN first_web_touch IS NOT NULL AND first_mobile_touch IS NOT NULL THEN
      |        CASE
      |          WHEN first_web_touch < first_mobile_touch THEN 'Web First'
      |          ELSE 'Mobile First'
      |        END
      |      WHEN first_web_touch IS NOT NULL THEN 'Web Only'
      |      WHEN first_mobile_touch IS NOT NULL THEN 'Mobile Only'
      |      ELSE 'Direct'
      |    END as first_touch_attribution,
      |    CASE
      |      WHEN last_web_touch IS NOT NULL AND last_mobile_touch IS NOT NULL THEN
      |        CASE
      |          WHEN last_web_touch > last_mobile_touch THEN 'Web Last'
      |          ELSE 'Mobile Last'
      |        END
      |      WHEN last_web_touch IS NOT NULL THEN 'Web Only'
      |      WHEN last_mobile_touch IS NOT NULL THEN 'Mobile Only'
      |      ELSE 'Direct'
      |    END as last_touch_attribution
      |  FROM user_touchpoints
      |)
      |SELECT
      |  first_touch_attribution,
      |  last_touch_attribution,
      |  COUNT(*) as transaction_count,
      |  SUM(amount) as total_revenue,
      |  AVG(amount) as avg_transaction_value,
      |  AVG(web_sessions_before) as avg_web_sessions,
      |  AVG(mobile_sessions_before) as avg_mobile_sessions,
      |  AVG(activities_before) as avg_activities
      |FROM attribution_model
      |GROUP BY first_touch_attribution, last_touch_attribution
      |ORDER BY total_revenue DESC
    """.stripMargin
  )

  def query10_CrossPlatformPerformanceAnalysis = {
    val partitionFilter = getPartitionFilter(6) // Last 6 hours for performance analysis
    BMTQuery(
      "Cross-Platform Performance Analysis - Compare performance metrics across platforms",
      s"""
        |WITH performance_metrics AS (
        |  SELECT
        |    'web' as platform,
        |    year, month, day, hour, minute_bucket,
        |    COUNT(*) as total_events,
        |    COUNT(DISTINCT user_id) as unique_users,
        |    COUNT(DISTINCT session_id) as unique_sessions,
        |    AVG(page_load_time) as avg_load_time,
        |    PERCENTILE_APPROX(page_load_time, 0.95) as p95_load_time,
        |    SUM(CASE WHEN conversion = true THEN 1 ELSE 0 END) as conversions,
        |    SUM(revenue) as total_revenue,
        |    COUNT(DISTINCT device_id) as unique_devices,
        |    COUNT(DISTINCT ip_address) as unique_ips
        |  FROM web_traffic
        |  WHERE ${partitionFilter}
        |  GROUP BY year, month, day, hour, minute_bucket
        |  UNION ALL
        |  SELECT
        |    'mobile' as platform,
        |    year, month, day, hour, minute_bucket,
        |    COUNT(*) as total_events,
        |    COUNT(DISTINCT user_id) as unique_users,
        |    COUNT(DISTINCT session_id) as unique_sessions,
        |    AVG(session_duration) as avg_load_time,
        |    PERCENTILE_APPROX(session_duration, 0.95) as p95_load_time,
        |    SUM(CASE WHEN action_type = 'tap' THEN 1 ELSE 0 END) as conversions,
        |    0 as total_revenue,
        |    COUNT(DISTINCT device_id) as unique_devices,
        |    COUNT(DISTINCT ip_address) as unique_ips
        |  FROM mobile_traffic
        |  WHERE ${partitionFilter}
        |  GROUP BY year, month, day, hour, minute_bucket
        |),
        |cross_platform_summary AS (
        |  SELECT
        |    pm.year, pm.month, pm.day, pm.hour, pm.minute_bucket,
      |    SUM(CASE WHEN pm.platform = 'web' THEN pm.total_events ELSE 0 END) as web_events,
      |    SUM(CASE WHEN pm.platform = 'mobile' THEN pm.total_events ELSE 0 END) as mobile_events,
      |    SUM(CASE WHEN pm.platform = 'web' THEN pm.unique_users ELSE 0 END) as web_users,
      |    SUM(CASE WHEN pm.platform = 'mobile' THEN pm.unique_users ELSE 0 END) as mobile_users,
      |    AVG(CASE WHEN pm.platform = 'web' THEN pm.avg_load_time END) as avg_web_load_time,
      |    AVG(CASE WHEN pm.platform = 'mobile' THEN pm.avg_load_time END) as avg_mobile_duration,
      |    SUM(pm.total_revenue) as total_revenue,
      |    COUNT(DISTINCT pm.platform) as platforms_active
      |  FROM performance_metrics pm
      |  GROUP BY pm.year, pm.month, pm.day, pm.hour, pm.minute_bucket
      |)
        |SELECT
        |  CONCAT(year, '-', month, '-', day, ' ', hour, ':', minute_bucket) as time_bucket,
        |  web_events,
        |  mobile_events,
        |  (web_events + mobile_events) as total_events,
        |  web_users,
        |  mobile_users,
        |  ROUND(100.0 * web_events / NULLIF(web_events + mobile_events, 0), 2) as web_event_percentage,
        |  ROUND(100.0 * mobile_events / NULLIF(web_events + mobile_events, 0), 2) as mobile_event_percentage,
        |  avg_web_load_time,
        |  avg_mobile_duration,
        |  total_revenue,
        |  platforms_active
        |FROM cross_platform_summary
        |ORDER BY year DESC, month DESC, day DESC, hour DESC, minute_bucket DESC
        |LIMIT 20
      """.stripMargin
    )
  }
}

// Case classes for data models
case class WebTrafficRecord(
                             event_id: String,
                             user_id: String,
                             session_id: String,
                             device_id: String,
                             timestamp: Long,
                             page_url: String,
                             referrer: String,
                             ip_address: String,
                             user_agent: String,
                             country: String,
                             city: String,
                             page_load_time: Int,
                             bounce_rate: Double,
                             conversion: Boolean,
                             revenue: Double,
                             browser: String,
                             os: String,
                             device_type: String,
                             // Partition columns
                             year: Int,
                             month: Int,
                             day: Int,
                             hour: Int,
                             minute_bucket: Int
                           )

case class MobileTrafficRecord(
                                event_id: String,
                                user_id: String,
                                session_id: String,
                                device_id: String,
                                timestamp: Long,
                                app_name: String,
                                app_version: String,
                                screen_name: String,
                                action_type: String,
                                ip_address: String,
                                country: String,
                                city: String,
                                network_type: String,
                                battery_level: Int,
                                memory_usage: Int,
                                cpu_usage: Double,
                                crash_occurred: Boolean,
                                session_duration: Int,
                                data_usage_mb: Double,
                                // Partition columns
                                year: Int,
                                month: Int,
                                day: Int,
                                hour: Int,
                                minute_bucket: Int
                              )

case class UserActivityRecord(
                               activity_id: String,
                               user_id: String,
                               session_id: String,
                               device_id: String,
                               timestamp: Long,
                               activity_type: String,
                               duration_seconds: Int,
                               ip_address: String,
                               country: String,
                               city: String,
                               product_id: String,
                               category_id: String,
                               subcategory_id: String,
                               tags: String,
                               score: Int,
                               is_premium_user: Boolean,
                               // Partition columns
                               year: Int,
                               month: Int,
                               day: Int,
                               hour: Int,
                               minute_bucket: Int
                             )

case class TransactionRecord(
                              transaction_id: String,
                              user_id: String,
                              session_id: String,
                              device_id: String,
                              timestamp: Long,
                              amount: Double,
                              currency: String,
                              payment_method: String,
                              merchant_id: String,
                              product_ids: String,
                              ip_address: String,
                              country: String,
                              city: String,
                              is_fraud: Boolean,
                              risk_score: Int,
                              processing_time_ms: Int,
                              status: String,
                              // Partition columns
                              year: Int,
                              month: Int,
                              day: Int,
                              hour: Int,
                              minute_bucket: Int
                            )