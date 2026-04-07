/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._

/**
 * Benchmark to measure cache performance with Arrow format vs Default format
 * using TPC-DS scale factor 1 data.
 *
 * Prerequisites:
 *   Generate TPC-DS data using dsdgen:
 *   {{{
 *     cd /path/to/tpcds-kit/tools
 *     ./dsdgen -SCALE 1 -DIR /tmp/tpcds-sf1 -FORCE Y -TERMINATE N
 *   }}}
 *
 * Then convert to Parquet:
 *   {{{
 *     build/sbt "sql/Test/runMain
 *       org.apache.spark.sql.execution.benchmark.TPCDSCacheBenchmark
 *       --prepare-data --csv-dir /tmp/tpcds-sf1 --parquet-dir /tmp/tpcds-sf1-parquet"
 *   }}}
 *
 * Run the benchmark:
 *   {{{
 *     build/sbt "sql/Test/runMain
 *       org.apache.spark.sql.execution.benchmark.TPCDSCacheBenchmark
 *       --data-dir /tmp/tpcds-sf1-parquet"
 *   }}}
 */
object TPCDSCacheBenchmark extends SqlBasedBenchmark {

  // TPC-DS table schemas (column name -> type) for CSV loading
  private val tableSchemas: Map[String, StructType] = Map(
    "store_sales" -> StructType(Seq(
      StructField("ss_sold_date_sk", IntegerType),
      StructField("ss_sold_time_sk", IntegerType),
      StructField("ss_item_sk", IntegerType),
      StructField("ss_customer_sk", IntegerType),
      StructField("ss_cdemo_sk", IntegerType),
      StructField("ss_hdemo_sk", IntegerType),
      StructField("ss_addr_sk", IntegerType),
      StructField("ss_store_sk", IntegerType),
      StructField("ss_promo_sk", IntegerType),
      StructField("ss_ticket_number", IntegerType),
      StructField("ss_quantity", IntegerType),
      StructField("ss_wholesale_cost", DecimalType(7, 2)),
      StructField("ss_list_price", DecimalType(7, 2)),
      StructField("ss_sales_price", DecimalType(7, 2)),
      StructField("ss_ext_discount_amt", DecimalType(7, 2)),
      StructField("ss_ext_sales_price", DecimalType(7, 2)),
      StructField("ss_ext_wholesale_cost", DecimalType(7, 2)),
      StructField("ss_ext_list_price", DecimalType(7, 2)),
      StructField("ss_ext_tax", DecimalType(7, 2)),
      StructField("ss_coupon_amt", DecimalType(7, 2)),
      StructField("ss_net_paid", DecimalType(7, 2)),
      StructField("ss_net_paid_inc_tax", DecimalType(7, 2)),
      StructField("ss_net_profit", DecimalType(7, 2))
    )),
    "date_dim" -> StructType(Seq(
      StructField("d_date_sk", IntegerType),
      StructField("d_date_id", StringType),
      StructField("d_date", DateType),
      StructField("d_month_seq", IntegerType),
      StructField("d_week_seq", IntegerType),
      StructField("d_quarter_seq", IntegerType),
      StructField("d_year", IntegerType),
      StructField("d_dow", IntegerType),
      StructField("d_moy", IntegerType),
      StructField("d_dom", IntegerType),
      StructField("d_qoy", IntegerType),
      StructField("d_fy_year", IntegerType),
      StructField("d_fy_quarter_seq", IntegerType),
      StructField("d_fy_week_seq", IntegerType),
      StructField("d_day_name", StringType),
      StructField("d_quarter_name", StringType),
      StructField("d_holiday", StringType),
      StructField("d_weekend", StringType),
      StructField("d_following_holiday", StringType),
      StructField("d_first_dom", IntegerType),
      StructField("d_last_dom", IntegerType),
      StructField("d_same_day_ly", IntegerType),
      StructField("d_same_day_lq", IntegerType),
      StructField("d_current_day", StringType),
      StructField("d_current_week", StringType),
      StructField("d_current_month", StringType),
      StructField("d_current_quarter", StringType),
      StructField("d_current_year", StringType)
    )),
    "item" -> StructType(Seq(
      StructField("i_item_sk", IntegerType),
      StructField("i_item_id", StringType),
      StructField("i_rec_start_date", DateType),
      StructField("i_rec_end_date", DateType),
      StructField("i_item_desc", StringType),
      StructField("i_current_price", DecimalType(7, 2)),
      StructField("i_wholesale_cost", DecimalType(7, 2)),
      StructField("i_brand_id", IntegerType),
      StructField("i_brand", StringType),
      StructField("i_class_id", IntegerType),
      StructField("i_class", StringType),
      StructField("i_category_id", IntegerType),
      StructField("i_category", StringType),
      StructField("i_manufact_id", IntegerType),
      StructField("i_manufact", StringType),
      StructField("i_size", StringType),
      StructField("i_formulation", StringType),
      StructField("i_color", StringType),
      StructField("i_units", StringType),
      StructField("i_container", StringType),
      StructField("i_manager_id", IntegerType),
      StructField("i_product_name", StringType)
    )),
    "household_demographics" -> StructType(Seq(
      StructField("hd_demo_sk", IntegerType),
      StructField("hd_income_band_sk", IntegerType),
      StructField("hd_buy_potential", StringType),
      StructField("hd_dep_count", IntegerType),
      StructField("hd_vehicle_count", IntegerType)
    )),
    "time_dim" -> StructType(Seq(
      StructField("t_time_sk", IntegerType),
      StructField("t_time_id", StringType),
      StructField("t_time", IntegerType),
      StructField("t_hour", IntegerType),
      StructField("t_minute", IntegerType),
      StructField("t_second", IntegerType),
      StructField("t_am_pm", StringType),
      StructField("t_shift", StringType),
      StructField("t_sub_shift", StringType),
      StructField("t_meal_time", StringType)
    )),
    "store" -> StructType(Seq(
      StructField("s_store_sk", IntegerType),
      StructField("s_store_id", StringType),
      StructField("s_rec_start_date", DateType),
      StructField("s_rec_end_date", DateType),
      StructField("s_closed_date_sk", IntegerType),
      StructField("s_store_name", StringType),
      StructField("s_number_employees", IntegerType),
      StructField("s_floor_space", IntegerType),
      StructField("s_hours", StringType),
      StructField("s_manager", StringType),
      StructField("s_market_id", IntegerType),
      StructField("s_geography_class", StringType),
      StructField("s_market_desc", StringType),
      StructField("s_market_manager", StringType),
      StructField("s_division_id", IntegerType),
      StructField("s_division_name", StringType),
      StructField("s_company_id", IntegerType),
      StructField("s_company_name", StringType),
      StructField("s_street_number", StringType),
      StructField("s_street_name", StringType),
      StructField("s_street_type", StringType),
      StructField("s_suite_number", StringType),
      StructField("s_city", StringType),
      StructField("s_county", StringType),
      StructField("s_state", StringType),
      StructField("s_zip", StringType),
      StructField("s_country", StringType),
      StructField("s_gmt_offset", DecimalType(5, 2)),
      StructField("s_tax_percentage", DecimalType(5, 2))
    )),
    "customer" -> StructType(Seq(
      StructField("c_customer_sk", IntegerType),
      StructField("c_customer_id", StringType),
      StructField("c_current_cdemo_sk", IntegerType),
      StructField("c_current_hdemo_sk", IntegerType),
      StructField("c_current_addr_sk", IntegerType),
      StructField("c_first_shipto_date_sk", IntegerType),
      StructField("c_first_sales_date_sk", IntegerType),
      StructField("c_salutation", StringType),
      StructField("c_first_name", StringType),
      StructField("c_last_name", StringType),
      StructField("c_preferred_cust_flag", StringType),
      StructField("c_birth_day", IntegerType),
      StructField("c_birth_month", IntegerType),
      StructField("c_birth_year", IntegerType),
      StructField("c_birth_country", StringType),
      StructField("c_login", StringType),
      StructField("c_email_address", StringType),
      StructField("c_last_review_date", IntegerType)
    )),
    "customer_address" -> StructType(Seq(
      StructField("ca_address_sk", IntegerType),
      StructField("ca_address_id", StringType),
      StructField("ca_street_number", StringType),
      StructField("ca_street_name", StringType),
      StructField("ca_street_type", StringType),
      StructField("ca_suite_number", StringType),
      StructField("ca_city", StringType),
      StructField("ca_county", StringType),
      StructField("ca_state", StringType),
      StructField("ca_zip", StringType),
      StructField("ca_country", StringType),
      StructField("ca_gmt_offset", DecimalType(5, 2)),
      StructField("ca_location_type", StringType)
    )),
    "customer_demographics" -> StructType(Seq(
      StructField("cd_demo_sk", IntegerType),
      StructField("cd_gender", StringType),
      StructField("cd_marital_status", StringType),
      StructField("cd_education_status", StringType),
      StructField("cd_purchase_estimate", IntegerType),
      StructField("cd_credit_rating", StringType),
      StructField("cd_dep_count", IntegerType),
      StructField("cd_dep_employed_count", IntegerType),
      StructField("cd_dep_college_count", IntegerType)
    )),
    "promotion" -> StructType(Seq(
      StructField("p_promo_sk", IntegerType),
      StructField("p_promo_id", StringType),
      StructField("p_start_date_sk", IntegerType),
      StructField("p_end_date_sk", IntegerType),
      StructField("p_item_sk", IntegerType),
      StructField("p_cost", DecimalType(15, 2)),
      StructField("p_response_target", IntegerType),
      StructField("p_promo_name", StringType),
      StructField("p_channel_dmail", StringType),
      StructField("p_channel_email", StringType),
      StructField("p_channel_catalog", StringType),
      StructField("p_channel_tv", StringType),
      StructField("p_channel_radio", StringType),
      StructField("p_channel_press", StringType),
      StructField("p_channel_event", StringType),
      StructField("p_channel_demo", StringType),
      StructField("p_channel_details", StringType),
      StructField("p_purpose", StringType),
      StructField("p_discount_active", StringType)
    ))
  )

  // Tables needed for our benchmark queries
  private val benchmarkTables = Seq(
    "store_sales", "date_dim", "item", "store",
    "household_demographics", "time_dim",
    "customer", "customer_address", "customer_demographics", "promotion"
  )

  // TPC-DS queries for benchmarking (simplified subset)
  private val benchmarkQueries: Seq[(String, String)] = Seq(
    // q3: 3-table join, filter + aggregation
    "q3" ->
      """SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,
        |  SUM(ss_ext_sales_price) sum_agg
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |  AND store_sales.ss_item_sk = item.i_item_sk
        |  AND item.i_manufact_id = 128
        |  AND dt.d_moy = 11
        |GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        |ORDER BY dt.d_year, sum_agg DESC, brand_id
        |LIMIT 100""".stripMargin,

    // q42: 3-table join, category aggregation
    "q42" ->
      """SELECT dt.d_year, item.i_category_id, item.i_category,
        |  sum(ss_ext_sales_price)
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |  AND store_sales.ss_item_sk = item.i_item_sk
        |  AND item.i_manager_id = 1
        |  AND dt.d_moy = 11
        |  AND dt.d_year = 2000
        |GROUP BY dt.d_year, item.i_category_id, item.i_category
        |ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year,
        |  item.i_category_id, item.i_category
        |LIMIT 100""".stripMargin,

    // q52: 3-table join, brand aggregation
    "q52" ->
      """SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,
        |  sum(ss_ext_sales_price) ext_price
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |  AND store_sales.ss_item_sk = item.i_item_sk
        |  AND item.i_manager_id = 1
        |  AND dt.d_moy = 11
        |  AND dt.d_year = 2000
        |GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        |ORDER BY dt.d_year, ext_price DESC, brand_id
        |LIMIT 100""".stripMargin,

    // q55: 3-table join, brand aggregation (different filter)
    "q55" ->
      """SELECT i_brand_id brand_id, i_brand brand,
        |  sum(ss_ext_sales_price) ext_price
        |FROM date_dim, store_sales, item
        |WHERE d_date_sk = ss_sold_date_sk
        |  AND ss_item_sk = i_item_sk
        |  AND i_manager_id = 28
        |  AND d_moy = 11
        |  AND d_year = 1999
        |GROUP BY i_brand, i_brand_id
        |ORDER BY ext_price DESC, brand_id
        |LIMIT 100""".stripMargin,

    // q96: 4-table join, count aggregation
    "q96" ->
      """SELECT count(*)
        |FROM store_sales, household_demographics, time_dim, store
        |WHERE ss_sold_time_sk = time_dim.t_time_sk
        |  AND ss_hdemo_sk = household_demographics.hd_demo_sk
        |  AND ss_store_sk = s_store_sk
        |  AND time_dim.t_hour = 20
        |  AND time_dim.t_minute >= 30
        |  AND household_demographics.hd_dep_count = 7
        |  AND store.s_store_name = 'ese'
        |ORDER BY count(*)
        |LIMIT 100""".stripMargin
  )

  private def createFreshSession(serializer: String): SparkSession = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    org.apache.spark.sql.execution.columnar.InMemoryRelation.clearSerializer()

    SparkSession.builder()
      .master("local[1]")
      .appName(s"TPCDSCacheBenchmark-$serializer")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 4)
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, (20 * 1024 * 1024).toString)
      .config(UI_ENABLED.key, false)
      .config(StaticSQLConf.SPARK_CACHE_SERIALIZER.key, serializer)
      .getOrCreate()
  }

  private def prepareParquetData(csvDir: String, parquetDir: String): Unit = {
    val spark = createFreshSession(
      "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
    try {
      benchmarkTables.foreach { tableName =>
        val csvPath = s"$csvDir/$tableName.dat"
        val parquetPath = s"$parquetDir/$tableName"
        // scalastyle:off println
        println(s"Converting $tableName from CSV to Parquet...")
        // scalastyle:on println
        val df = spark.read
          .option("delimiter", "|")
          .option("header", "false")
          .option("emptyValue", "")
          .schema(tableSchemas(tableName))
          .csv(csvPath)
        df.write.mode(SaveMode.Overwrite).parquet(parquetPath)
        // scalastyle:off println
        println(s"  $tableName: ${df.count()} rows")
        // scalastyle:on println
      }
    } finally {
      spark.stop()
    }
  }

  private def loadAndCacheTables(spark: SparkSession, dataDir: String): Unit = {
    benchmarkTables.foreach { tableName =>
      val df = spark.read.parquet(s"$dataDir/$tableName")
      df.createOrReplaceTempView(tableName)
      spark.catalog.cacheTable(tableName)
    }
    // Materialize all caches
    benchmarkTables.foreach { tableName =>
      spark.table(tableName).write.format("noop").mode("overwrite").save()
    }
  }

  private def uncacheAllTables(spark: SparkSession): Unit = {
    benchmarkTables.foreach { tableName =>
      spark.catalog.uncacheTable(tableName)
    }
  }

  private def runQueryBenchmarks(dataDir: String): Unit = {
    // store_sales has ~2.88M rows at SF1
    val numRows = 2880404L

    benchmarkQueries.foreach { case (queryName, querySQL) =>
      runBenchmark(s"TPC-DS $queryName (cached, query-only)") {
        val benchmark = new Benchmark(
          s"TPC-DS $queryName query-only on cached SF1", numRows, 5, output = output)

        // Default cache (compressed - default)
        benchmark.addTimerCase(s"Default cache (compressed)") { timer =>
          val spark = createFreshSession(
            "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
          try {
            loadAndCacheTables(spark, dataDir)
            // Warm up: run query once to compile codegen etc.
            spark.sql(querySQL).write.format("noop").mode("overwrite").save()
            timer.startTiming()
            spark.sql(querySQL).write.format("noop").mode("overwrite").save()
            timer.stopTiming()
            uncacheAllTables(spark)
          } finally {
            spark.stop()
          }
        }

        // Arrow cache (no compression)
        benchmark.addTimerCase(s"Arrow cache (no compression)") { timer =>
          val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
          try {
            loadAndCacheTables(spark, dataDir)
            spark.sql(querySQL).write.format("noop").mode("overwrite").save()
            timer.startTiming()
            spark.sql(querySQL).write.format("noop").mode("overwrite").save()
            timer.stopTiming()
            uncacheAllTables(spark)
          } finally {
            spark.stop()
          }
        }

        // Arrow cache (zstd level 3)
        benchmark.addTimerCase(s"Arrow cache (zstd level 3)") { timer =>
          val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
          try {
            spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
            spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
            loadAndCacheTables(spark, dataDir)
            spark.sql(querySQL).write.format("noop").mode("overwrite").save()
            timer.startTiming()
            spark.sql(querySQL).write.format("noop").mode("overwrite").save()
            timer.stopTiming()
            uncacheAllTables(spark)
          } finally {
            spark.stop()
          }
        }

        benchmark.run()
      }
    }
  }

  private def runCacheWriteReadBenchmark(dataDir: String): Unit = {
    val numRows = 2880404L

    // Benchmark 1: Cache build (write) time
    runBenchmark("TPC-DS store_sales cache build") {
      val benchmark = new Benchmark(
        "Cache build store_sales (2.88M rows, 23 cols)", numRows, 3, output = output)

      benchmark.addCase("Default cache (compressed)") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Default cache (uncompressed)") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false")
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (no compression)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (zstd level 3)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
          spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }

    // Benchmark 2: Cache read (scan cached data) time
    runBenchmark("TPC-DS store_sales cache read") {
      val benchmark = new Benchmark(
        "Read cached store_sales (2.88M rows, 23 cols)", numRows, 5, output = output)

      benchmark.addTimerCase("Default cache (compressed)") { timer =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save() // build cache
          timer.startTiming()
          df.write.format("noop").mode("overwrite").save() // read from cache
          timer.stopTiming()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addTimerCase("Default cache (uncompressed)") { timer =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false")
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          timer.startTiming()
          df.write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addTimerCase("Arrow cache (no compression)") { timer =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          timer.startTiming()
          df.write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addTimerCase("Arrow cache (zstd level 3)") { timer =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
          spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          timer.startTiming()
          df.write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }
  }

  /**
   * Benchmark: read cached store_sales with only 3 primitive INT columns vs all 23 columns.
   * This isolates whether column count/type (especially Decimal) is the cause of the
   * performance gap between Arrow cache and Default cache.
   */
  private def runNarrowVsWideScanBenchmark(dataDir: String): Unit = {
    val numRows = 2880404L

    // Narrow scan: 3 INT columns only
    runBenchmark("TPC-DS store_sales cache read - 3 INT columns") {
      val benchmark = new Benchmark(
        "Read cached store_sales (3 INT cols)", numRows, 5, output = output)

      val selectSQL = "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk FROM store_sales"

      benchmark.addTimerCase("Default cache (compressed)") { timer =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.createOrReplaceTempView("store_sales")
          spark.catalog.cacheTable("store_sales")
          spark.sql("SELECT * FROM store_sales").write.format("noop").mode("overwrite").save()
          // warm up
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.startTiming()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          spark.catalog.uncacheTable("store_sales")
        } finally {
          spark.stop()
        }
      }

      benchmark.addTimerCase("Arrow cache (no compression)") { timer =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.createOrReplaceTempView("store_sales")
          spark.catalog.cacheTable("store_sales")
          spark.sql("SELECT * FROM store_sales").write.format("noop").mode("overwrite").save()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.startTiming()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          spark.catalog.uncacheTable("store_sales")
        } finally {
          spark.stop()
        }
      }

      benchmark.addTimerCase("Arrow cache (zstd level 3)") { timer =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
          spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.createOrReplaceTempView("store_sales")
          spark.catalog.cacheTable("store_sales")
          spark.sql("SELECT * FROM store_sales").write.format("noop").mode("overwrite").save()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.startTiming()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          spark.catalog.uncacheTable("store_sales")
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }

    // Wide scan: all 23 columns
    runBenchmark("TPC-DS store_sales cache read - all 23 columns") {
      val benchmark = new Benchmark(
        "Read cached store_sales (all 23 cols)", numRows, 5, output = output)

      val selectSQL = "SELECT * FROM store_sales"

      benchmark.addTimerCase("Default cache (compressed)") { timer =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.createOrReplaceTempView("store_sales")
          spark.catalog.cacheTable("store_sales")
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          // warm up
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.startTiming()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          spark.catalog.uncacheTable("store_sales")
        } finally {
          spark.stop()
        }
      }

      benchmark.addTimerCase("Arrow cache (no compression)") { timer =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.createOrReplaceTempView("store_sales")
          spark.catalog.cacheTable("store_sales")
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.startTiming()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          spark.catalog.uncacheTable("store_sales")
        } finally {
          spark.stop()
        }
      }

      benchmark.addTimerCase("Arrow cache (zstd level 3)") { timer =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
          spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.createOrReplaceTempView("store_sales")
          spark.catalog.cacheTable("store_sales")
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.startTiming()
          spark.sql(selectSQL).write.format("noop").mode("overwrite").save()
          timer.stopTiming()
          spark.catalog.uncacheTable("store_sales")
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }
  }

  /**
   * Run benchmarks in the exact same style as ArrowCacheBenchmark (write+read mixed timing)
   * but on TPC-DS store_sales data instead of spark.range() synthetic data.
   * Also includes a spark.range() control group for direct comparison.
   */
  private def runMicroStyleBenchmark(dataDir: String): Unit = {
    val numRows = 2880404L

    // Control group: spark.range() with 3 primitive columns (same as ArrowCacheBenchmark)
    runBenchmark("Control: spark.range 3M rows, 3 primitives (write+read)") {
      val benchmark = new Benchmark(
        "spark.range 3M rows, 3 primitives", 3000000L, output = output)

      benchmark.addCase("Default cache (compressed)") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.range(3000000L).selectExpr(
            "id as int_col", "id * 2L as long_col", "cast(id as double) as double_col")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (no compression)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.range(3000000L).selectExpr(
            "id as int_col", "id * 2L as long_col", "cast(id as double) as double_col")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (zstd level 3)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
          spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
          val df = spark.range(3000000L).selectExpr(
            "id as int_col", "id * 2L as long_col", "cast(id as double) as double_col")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }

    // Test group: TPC-DS store_sales, select 3 INT columns (write+read mixed)
    runBenchmark("TPC-DS store_sales 3 INT cols (write+read)") {
      val benchmark = new Benchmark(
        "store_sales 3 INT cols write+read", numRows, output = output)

      benchmark.addCase("Default cache (compressed)") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
            .selectExpr("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (no compression)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
            .selectExpr("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (zstd level 3)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
          spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
          val df = spark.read.parquet(s"$dataDir/store_sales")
            .selectExpr("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }

    // Test group: TPC-DS store_sales, all 23 columns (write+read mixed)
    runBenchmark("TPC-DS store_sales all 23 cols (write+read)") {
      val benchmark = new Benchmark(
        "store_sales all 23 cols write+read", numRows, output = output)

      benchmark.addCase("Default cache (compressed)") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (no compression)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache (zstd level 3)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
          spark.conf.set("spark.sql.execution.arrow.compression.level", "3")
          val df = spark.read.parquet(s"$dataDir/store_sales")
          df.cache()
          df.write.format("noop").mode("overwrite").save()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }
  }

  /**
   * Split write vs read timing for 3 INT columns and all 23 columns,
   * with all 4 cache configurations.
   */
  /**
   * Test whether the performance gap comes from row-input vs columnar-input path.
   * spark.range() -- row input (convertInternalRowToCachedBatch)
   * Parquet read -- columnar input (convertColumnarBatchToCachedBatch)
   *
   * We test both with 23 columns to see if Parquet (columnar input) is the cause.
   */
  private def runInputPathTest(dataDir: String): Unit = {
    val numRows = 2880404L

    val configs: Seq[(String, String, Map[String, String])] = Seq(
      ("Default (compressed)",
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer",
        Map.empty),
      ("Arrow (no compression)",
        classOf[ArrowCachedBatchSerializer].getName,
        Map.empty),
      ("Arrow (zstd level 3)",
        classOf[ArrowCachedBatchSerializer].getName,
        Map("spark.sql.execution.arrow.compression.codec" -> "zstd",
          "spark.sql.execution.arrow.compression.level" -> "3"))
    )

    // Row input: spark.range() with 3 columns (same as ArrowCacheBenchmark)
    runBenchmark("Row input: spark.range 3 cols (write+read)") {
      val benchmark = new Benchmark(
        "spark.range 3 cols write+read", numRows, output = output)

      for ((name, serializer, extraConf) <- configs) {
        benchmark.addCase(name) { _ =>
          val spark = createFreshSession(serializer)
          try {
            extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
            val df = spark.range(numRows).selectExpr(
              "id as int_col", "id * 2L as long_col", "cast(id as double) as double_col")
            df.cache()
            df.write.format("noop").mode("overwrite").save()
            df.unpersist(blocking = true)
          } finally {
            spark.stop()
          }
        }
      }

      benchmark.run()
    }

    // Row input: spark.range() with 23 columns (mimics micro-benchmark but wider)
    runBenchmark("Row input: spark.range 23 cols (write+read)") {
      val benchmark = new Benchmark(
        "spark.range 23 cols write+read", numRows, output = output)

      for ((name, serializer, extraConf) <- configs) {
        benchmark.addCase(name) { _ =>
          val spark = createFreshSession(serializer)
          try {
            extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
            val df = spark.range(numRows).selectExpr(
              (0 until 10).map(i => s"cast((id % 10000) + $i as int) as int_col$i") ++
              (0 until 11).map(i =>
                s"cast((id % 10000 + $i) * 1.23 as decimal(7,2)) as dec_col$i") ++
              Seq("cast(id as double) as dbl_col", "cast(id as string) as str_col"): _*
            )
            df.cache()
            df.write.format("noop").mode("overwrite").save()
            df.unpersist(blocking = true)
          } finally {
            spark.stop()
          }
        }
      }

      benchmark.run()
    }

    // Columnar input: Parquet 3 INT columns
    runBenchmark("Columnar input: Parquet store_sales 3 cols (write+read)") {
      val benchmark = new Benchmark(
        "Parquet store_sales 3 cols write+read", numRows, output = output)

      for ((name, serializer, extraConf) <- configs) {
        benchmark.addCase(name) { _ =>
          val spark = createFreshSession(serializer)
          try {
            extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
            val df = spark.read.parquet(s"$dataDir/store_sales")
              .selectExpr("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk")
            df.cache()
            df.write.format("noop").mode("overwrite").save()
            df.unpersist(blocking = true)
          } finally {
            spark.stop()
          }
        }
      }

      benchmark.run()
    }

    // Columnar input: Parquet 23 columns (actual TPC-DS data)
    runBenchmark("Columnar input: Parquet store_sales 23 cols (write+read)") {
      val benchmark = new Benchmark(
        "Parquet store_sales 23 cols write+read", numRows, output = output)

      for ((name, serializer, extraConf) <- configs) {
        benchmark.addCase(name) { _ =>
          val spark = createFreshSession(serializer)
          try {
            extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
            val df = spark.read.parquet(s"$dataDir/store_sales")
            df.cache()
            df.write.format("noop").mode("overwrite").save()
            df.unpersist(blocking = true)
          } finally {
            spark.stop()
          }
        }
      }

      benchmark.run()
    }
  }

  /**
   * Benchmark pure columnar read from cache, bypassing columnar-to-row conversion.
   * Uses executeColumnar() to consume ColumnarBatch directly.
   */
  private def runColumnarReadBenchmark(dataDir: String): Unit = {
    val numRows = 2880404L

    val configs: Seq[(String, String, Map[String, String])] = Seq(
      ("Default (compressed)",
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer",
        Map.empty),
      ("Default (uncompressed)",
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer",
        Map("spark.sql.inMemoryColumnarStorage.compressed" -> "false")),
      ("Arrow (no compression)",
        classOf[ArrowCachedBatchSerializer].getName,
        Map.empty),
      ("Arrow (zstd level 3)",
        classOf[ArrowCachedBatchSerializer].getName,
        Map("spark.sql.execution.arrow.compression.codec" -> "zstd",
          "spark.sql.execution.arrow.compression.level" -> "3"))
    )

    // Helper: consume columnar batches directly from cache scan
    def consumeColumnar(spark: SparkSession, tableName: String): Unit = {
      import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
      val df = spark.table(tableName)
      val plan = df.queryExecution.executedPlan
      // Find the InMemoryTableScanExec in the plan
      val scanExec = plan.collectFirst {
        case scan: InMemoryTableScanExec => scan
      }.getOrElse(throw new RuntimeException("No InMemoryTableScanExec found"))

      // Execute columnar and consume all batches
      val rdd = scanExec.executeColumnar()
      rdd.foreach { batch =>
        // Just access numRows to force materialization
        batch.numRows()
      }
    }

    // 3 INT columns - columnar read
    runBenchmark("Columnar read store_sales 3 INT cols") {
      val benchmark = new Benchmark(
        "Columnar read 3 INT cols", numRows, 5, output = output)

      for ((name, serializer, extraConf) <- configs) {
        benchmark.addTimerCase(name) { timer =>
          val spark = createFreshSession(serializer)
          try {
            extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
            val df = spark.read.parquet(s"$dataDir/store_sales")
              .selectExpr("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk")
            df.createOrReplaceTempView("store_sales")
            spark.catalog.cacheTable("store_sales")
            // materialize cache
            spark.sql("SELECT * FROM store_sales")
              .write.format("noop").mode("overwrite").save()
            // warm up columnar read
            consumeColumnar(spark, "store_sales")
            timer.startTiming()
            consumeColumnar(spark, "store_sales")
            timer.stopTiming()
            spark.catalog.uncacheTable("store_sales")
          } finally {
            spark.stop()
          }
        }
      }

      benchmark.run()
    }

    // All 23 columns - columnar read
    // NOTE: Default cache does not support columnar output for Decimal/String types,
    // so only Arrow configs are tested here. Default uses row read for comparison.
    runBenchmark("Columnar read store_sales all 23 cols (Arrow only)") {
      val benchmark = new Benchmark(
        "Columnar read all 23 cols", numRows, 5, output = output)

      val arrowConfigs = configs.filter(_._1.startsWith("Arrow"))
      for ((name, serializer, extraConf) <- arrowConfigs) {
        benchmark.addTimerCase(name) { timer =>
          val spark = createFreshSession(serializer)
          try {
            extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
            val df = spark.read.parquet(s"$dataDir/store_sales")
            df.createOrReplaceTempView("store_sales")
            spark.catalog.cacheTable("store_sales")
            spark.sql("SELECT * FROM store_sales")
              .write.format("noop").mode("overwrite").save()
            consumeColumnar(spark, "store_sales")
            timer.startTiming()
            consumeColumnar(spark, "store_sales")
            timer.stopTiming()
            spark.catalog.uncacheTable("store_sales")
          } finally {
            spark.stop()
          }
        }
      }

      benchmark.run()
    }

    // Row read for comparison (same as before, via noop)
    runBenchmark("Row read store_sales all 23 cols (for comparison)") {
      val benchmark = new Benchmark(
        "Row read all 23 cols", numRows, 5, output = output)

      for ((name, serializer, extraConf) <- configs) {
        benchmark.addTimerCase(name) { timer =>
          val spark = createFreshSession(serializer)
          try {
            extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
            val df = spark.read.parquet(s"$dataDir/store_sales")
            df.createOrReplaceTempView("store_sales")
            spark.catalog.cacheTable("store_sales")
            spark.sql("SELECT * FROM store_sales")
              .write.format("noop").mode("overwrite").save()
            // warm up
            spark.sql("SELECT * FROM store_sales")
              .write.format("noop").mode("overwrite").save()
            timer.startTiming()
            spark.sql("SELECT * FROM store_sales")
              .write.format("noop").mode("overwrite").save()
            timer.stopTiming()
            spark.catalog.uncacheTable("store_sales")
          } finally {
            spark.stop()
          }
        }
      }

      benchmark.run()
    }
  }

  private def runWriteReadSplitBenchmark(dataDir: String): Unit = {
    val numRows = 2880404L

    val configs: Seq[(String, String, Map[String, String])] = Seq(
      ("Default (compressed)",
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer",
        Map.empty),
      ("Default (uncompressed)",
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer",
        Map("spark.sql.inMemoryColumnarStorage.compressed" -> "false")),
      ("Arrow (no compression)",
        classOf[ArrowCachedBatchSerializer].getName,
        Map.empty),
      ("Arrow (zstd level 3)",
        classOf[ArrowCachedBatchSerializer].getName,
        Map("spark.sql.execution.arrow.compression.codec" -> "zstd",
          "spark.sql.execution.arrow.compression.level" -> "3"))
    )

    case class ScanDef(label: String, selectExpr: String)
    val scans = Seq(
      ScanDef("3 INT cols",
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk FROM store_sales"),
      ScanDef("10 INT cols (no Decimal)",
        """SELECT ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk,
          |  ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk,
          |  ss_ticket_number FROM store_sales""".stripMargin),
      ScanDef("all 23 cols",
        "SELECT * FROM store_sales")
    )

    for (scan <- scans) {
      // --- WRITE benchmark ---
      runBenchmark(s"store_sales WRITE ${scan.label}") {
        val benchmark = new Benchmark(
          s"Cache write store_sales ${scan.label}", numRows, 3, output = output)

        for ((name, serializer, extraConf) <- configs) {
          benchmark.addCase(name) { _ =>
            val spark = createFreshSession(serializer)
            try {
              extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
              val df = spark.read.parquet(s"$dataDir/store_sales")
              df.createOrReplaceTempView("store_sales")
              val selected = spark.sql(scan.selectExpr)
              selected.cache()
              selected.write.format("noop").mode("overwrite").save()
              selected.unpersist(blocking = true)
            } finally {
              spark.stop()
            }
          }
        }

        benchmark.run()
      }

      // --- READ benchmark ---
      runBenchmark(s"store_sales READ ${scan.label}") {
        val benchmark = new Benchmark(
          s"Cache read store_sales ${scan.label}", numRows, 5, output = output)

        for ((name, serializer, extraConf) <- configs) {
          benchmark.addTimerCase(name) { timer =>
            val spark = createFreshSession(serializer)
            try {
              extraConf.foreach { case (k, v) => spark.conf.set(k, v) }
              val df = spark.read.parquet(s"$dataDir/store_sales")
              df.createOrReplaceTempView("store_sales")
              val selected = spark.sql(scan.selectExpr)
              selected.cache()
              selected.write.format("noop").mode("overwrite").save() // build cache
              // warm up read
              selected.write.format("noop").mode("overwrite").save()
              timer.startTiming()
              selected.write.format("noop").mode("overwrite").save() // timed read
              timer.stopTiming()
              selected.unpersist(blocking = true)
            } finally {
              spark.stop()
            }
          }
        }

        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val args = mainArgs.toList

    // Check for --prepare-data mode
    val prepareIdx = args.indexOf("--prepare-data")
    if (prepareIdx >= 0) {
      val csvDirIdx = args.indexOf("--csv-dir")
      val parquetDirIdx = args.indexOf("--parquet-dir")
      require(csvDirIdx >= 0 && parquetDirIdx >= 0,
        "Usage: --prepare-data --csv-dir <path> --parquet-dir <path>")
      val csvDir = args(csvDirIdx + 1)
      val parquetDir = args(parquetDirIdx + 1)
      prepareParquetData(csvDir, parquetDir)
      return
    }

    val dataDirIdx = args.indexOf("--data-dir")
    require(dataDirIdx >= 0, "Usage: --data-dir <path-to-tpcds-parquet-data>")
    val dataDir = args(dataDirIdx + 1)

    if (args.contains("--columnar-read")) {
      runBenchmark("Columnar Read Benchmark (SF1)") {
        runColumnarReadBenchmark(dataDir)
      }
    } else if (args.contains("--input-path-test")) {
      runBenchmark("Input Path Test: Row vs Columnar") {
        runInputPathTest(dataDir)
      }
    } else if (args.contains("--write-read-split")) {
      runBenchmark("TPC-DS Write/Read Split Benchmark (SF1)") {
        runWriteReadSplitBenchmark(dataDir)
      }
    } else if (args.contains("--narrow-wide-only")) {
      runBenchmark("TPC-DS Narrow vs Wide Scan Benchmark (SF1)") {
        runNarrowVsWideScanBenchmark(dataDir)
      }
    } else if (args.contains("--micro-style-only")) {
      runBenchmark("Micro-style Benchmark on TPC-DS data") {
        runMicroStyleBenchmark(dataDir)
      }
    } else {
      runBenchmark("TPC-DS Cache Benchmark (SF1)") {
        runCacheWriteReadBenchmark(dataDir)
        runNarrowVsWideScanBenchmark(dataDir)
        runQueryBenchmarks(dataDir)
      }
    }
  }
}
