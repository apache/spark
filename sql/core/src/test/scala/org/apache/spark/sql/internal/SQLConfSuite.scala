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

package org.apache.spark.sql.internal

import java.util.{Locale, TimeZone}

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.{SPARK_DOC_ROOT, SparkIllegalArgumentException, SparkNoSuchElementException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.MIT
import org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodec.{GZIP, LZO}
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.test.{SharedSparkSession, TestSQLContext}
import org.apache.spark.util.Utils

class SQLConfSuite extends QueryTest with SharedSparkSession {

  private val testKey = "test.key.0"
  private val testVal = "test.val.0"

  test("propagate from spark conf") {
    // We create a new context here to avoid order dependence with other tests that might call
    // clear().
    val newContext = new SQLContext(SparkSession.builder().sparkContext(sparkContext).getOrCreate())
    assert(newContext.getConf("spark.sql.testkey", "false") === "true")
  }

  test("programmatic ways of basic setting and getting") {
    // Set a conf first.
    spark.conf.set(testKey, testVal)
    // Clear the conf.
    sqlConf.clear()
    // After clear, only overrideConfs used by unit test should be in the SQLConf.
    assert(spark.conf.getAll === TestSQLContext.overrideConfs)

    spark.conf.set(testKey, testVal)
    assert(spark.conf.get(testKey) === testVal)
    assert(spark.conf.get(testKey, testVal + "_") === testVal)
    assert(spark.conf.getAll.contains(testKey))

    // Tests SQLConf as accessed from a SQLContext is mutable after
    // the latter is initialized, unlike SparkConf inside a SparkContext.
    assert(spark.conf.get(testKey) === testVal)
    assert(spark.conf.get(testKey, testVal + "_") === testVal)
    assert(spark.conf.getAll.contains(testKey))

    sqlConf.clear()
  }

  test("parse SQL set commands") {
    sqlConf.clear()
    sql(s"set $testKey=$testVal")
    assert(spark.conf.get(testKey, testVal + "_") === testVal)
    assert(spark.conf.get(testKey, testVal + "_") === testVal)

    sql("set some.property=20")
    assert(spark.conf.get("some.property", "0") === "20")
    sql("set some.property = 40")
    assert(spark.conf.get("some.property", "0") === "40")

    val key = "spark.sql.key"
    val vs = "val0,val_1,val2.3,my_table"
    sql(s"set $key=$vs")
    assert(spark.conf.get(key, "0") === vs)

    sql(s"set $key=")
    assert(spark.conf.get(key, "0") === "")

    sqlConf.clear()
  }

  test("set command for display") {
    sqlConf.clear()
    checkAnswer(
      sql("SET").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Nil)

    checkAnswer(
      sql("SET -v").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Row("spark.sql.groupByOrdinal", "true"))

    sql("SET spark.sql.groupByOrdinal=false")

    checkAnswer(
      sql("SET").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Row("spark.sql.groupByOrdinal", "false"))

    checkAnswer(
      sql("SET -v").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Row("spark.sql.groupByOrdinal", "false"))
  }

  test("deprecated property") {
    sqlConf.clear()
    val original = sqlConf.getConf(SQLConf.SHUFFLE_PARTITIONS)
    try {
      sql(s"set ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS}=10")
      assert(sqlConf.getConf(SQLConf.SHUFFLE_PARTITIONS) === 10)
    } finally {
      sql(s"set ${SQLConf.SHUFFLE_PARTITIONS.key}=$original")
    }
  }

  test(s"SPARK-35168: ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} should respect" +
      s" ${SQLConf.SHUFFLE_PARTITIONS.key}") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      checkAnswer(sql(s"SET ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS}"),
        Row(SQLConf.SHUFFLE_PARTITIONS.key, "2"))
    }
  }

  test("SPARK-31234: reset will not change static sql configs and spark core configs") {
    val conf = spark.sparkContext.getConf.getAll.toMap
    val appName = conf.get("spark.app.name")
    val driverHost = conf.get("spark.driver.host")
    val master = conf.get("spark.master")
    val warehouseDir = conf.get("spark.sql.warehouse.dir")
    // ensure the conf here is not default value, and will not be reset to default value later
    assert(warehouseDir.get.contains(this.getClass.getCanonicalName))
    sql("RESET")
    assert(conf.get("spark.app.name") === appName)
    assert(conf.get("spark.driver.host") === driverHost)
    assert(conf.get("spark.master") === master)
    assert(conf.get("spark.sql.warehouse.dir") === warehouseDir)
  }

  test("reset - public conf") {
    sqlConf.clear()
    val original = sqlConf.getConf(SQLConf.GROUP_BY_ORDINAL)
    try {
      assert(sqlConf.getConf(SQLConf.GROUP_BY_ORDINAL))
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL.key}=false")
      assert(sqlConf.getConf(SQLConf.GROUP_BY_ORDINAL) === false)
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 1)
      assert(sqlConf.getConf(SQLConf.OPTIMIZER_EXCLUDED_RULES).isEmpty)
      sql(s"reset")
      assert(sqlConf.getConf(SQLConf.GROUP_BY_ORDINAL))
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 0)
      assert(sqlConf.getConf(SQLConf.OPTIMIZER_EXCLUDED_RULES) ===
        Some("org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"))
    } finally {
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL.key}=$original")
    }
  }

  test("reset - internal conf") {
    sqlConf.clear()
    val original = sqlConf.getConf(SQLConf.OPTIMIZER_MAX_ITERATIONS)
    try {
      assert(sqlConf.getConf(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 100)
      sql(s"set ${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}=10")
      assert(sqlConf.getConf(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 10)
      assert(sql(s"set").where(s"key = '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}'").count() == 1)
      sql(s"reset")
      assert(sqlConf.getConf(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 100)
      assert(sql(s"set").where(s"key = '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}'").count() == 0)
    } finally {
      sql(s"set ${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}=$original")
    }
  }

  test("reset - user-defined conf") {
    sqlConf.clear()
    val userDefinedConf = "x.y.z.reset"
    try {
      assert(spark.conf.getOption(userDefinedConf).isEmpty)
      sql(s"set $userDefinedConf=false")
      assert(spark.conf.get(userDefinedConf) === "false")
      assert(sql(s"set").where(s"key = '$userDefinedConf'").count() == 1)
      sql(s"reset")
      assert(spark.conf.getOption(userDefinedConf).isEmpty)
    } finally {
      spark.conf.unset(userDefinedConf)
    }
  }

  test("SPARK-32406: reset - single configuration") {
    sqlConf.clear()
    // spark core conf w/o entry registered
    val appId = spark.sparkContext.getConf.getAppId
    sql("RESET spark.app.id")
    assert(spark.conf.get("spark.app.id") === appId, "Should not change spark core ones")
    // spark core conf w/ entry registered
    checkError(
      exception = intercept[AnalysisException](sql("RESET spark.executor.cores")),
      condition = "CANNOT_MODIFY_CONFIG",
      parameters = Map("key" -> "\"spark.executor.cores\"", "docroot" -> SPARK_DOC_ROOT)
    )

    // user defined settings
    sql("SET spark.abc=xyz")
    assert(spark.conf.get("spark.abc") === "xyz")
    sql("RESET spark.abc")
    intercept[NoSuchElementException](spark.conf.get("spark.abc"))
    sql("RESET spark.abc") // ignore nonexistent keys

    // runtime sql configs
    val original = sqlConf.getConf(SQLConf.GROUP_BY_ORDINAL)
    sql(s"SET ${SQLConf.GROUP_BY_ORDINAL.key}=false")
    sql(s"RESET ${SQLConf.GROUP_BY_ORDINAL.key}")
    assert(sqlConf.getConf(SQLConf.GROUP_BY_ORDINAL) === original)

    // runtime sql configs with optional defaults
    assert(sqlConf.getConf(SQLConf.OPTIMIZER_EXCLUDED_RULES).isEmpty)
    sql(s"RESET ${SQLConf.OPTIMIZER_EXCLUDED_RULES.key}")
    assert(sqlConf.getConf(SQLConf.OPTIMIZER_EXCLUDED_RULES) ===
      Some("org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"))
    sql(s"SET ${SQLConf.PLAN_CHANGE_LOG_RULES.key}=abc")
    sql(s"RESET ${SQLConf.PLAN_CHANGE_LOG_RULES.key}")
    assert(sqlConf.getConf(SQLConf.PLAN_CHANGE_LOG_RULES).isEmpty)

    // static sql configs
    checkError(
      exception = intercept[AnalysisException](sql(s"RESET ${StaticSQLConf.WAREHOUSE_PATH.key}")),
      condition = "_LEGACY_ERROR_TEMP_1325",
      parameters = Map("key" -> "spark.sql.warehouse.dir"))

  }

  test("invalid conf value") {
    val e = intercept[IllegalArgumentException] {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "10") {
      }
    }
    assert(e.getMessage === s"${SQLConf.CASE_SENSITIVE.key} should be boolean, but was 10")
  }

  test("Test ADVISORY_PARTITION_SIZE_IN_BYTES's method") {
    sqlConf.clear()

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "100")
    assert(sqlConf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 100)

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "1k")
    assert(sqlConf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 1024)

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "1M")
    assert(sqlConf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 1048576)

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "1g")
    assert(sqlConf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 1073741824)

    // test negative value
    intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "-1")
    }

    // Test overflow exception
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MaxValue
      spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "90000000000g")
    }

    intercept[IllegalArgumentException] {
      // This value less than Long.MinValue
      spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "-90000000000g")
    }

    sqlConf.clear()
  }

  test("SparkSession can access configs set in SparkConf") {
    try {
      sparkContext.conf.set("spark.to.be.or.not.to.be", "my love")
      sparkContext.conf.set("spark.sql.with.or.without.you", "my love")
      val spark = new SparkSession(sparkContext)
      assert(spark.conf.get("spark.to.be.or.not.to.be") == "my love")
      assert(spark.conf.get("spark.sql.with.or.without.you") == "my love")
    } finally {
      sparkContext.conf.remove("spark.to.be.or.not.to.be")
      sparkContext.conf.remove("spark.sql.with.or.without.you")
    }
  }

  test("default value of WAREHOUSE_PATH") {
    // JVM adds a trailing slash if the directory exists and leaves it as-is, if it doesn't
    // In our comparison, strip trailing slash off of both sides, to account for such cases
    assert(new Path(Utils.resolveURI("spark-warehouse")).toString.stripSuffix("/") === spark
      .sessionState.conf.warehousePath.stripSuffix("/"))
  }

  test("static SQL conf comes from SparkConf") {
    val previousValue = sparkContext.conf.get(GLOBAL_TEMP_DATABASE)
    try {
      sparkContext.conf.set(GLOBAL_TEMP_DATABASE, "a")
      val newSession = new SparkSession(sparkContext)
      assert(newSession.sessionState.conf.getConf(GLOBAL_TEMP_DATABASE) == "a")
      checkAnswer(
        newSession.sql(s"SET ${GLOBAL_TEMP_DATABASE.key}"),
        Row(GLOBAL_TEMP_DATABASE.key, "a"))
    } finally {
      sparkContext.conf.set(GLOBAL_TEMP_DATABASE, previousValue)
    }
  }

  test("cannot set/unset static SQL conf") {
    val e1 = intercept[AnalysisException](sql(s"SET ${GLOBAL_TEMP_DATABASE.key}=10"))
    assert(e1.message.contains("Cannot modify the value of a static config"))
    val e2 = intercept[AnalysisException](spark.conf.unset(GLOBAL_TEMP_DATABASE.key))
    assert(e2.message.contains("Cannot modify the value of a static config"))
  }

  test("SPARK-36643: Show migration guide when attempting SparkConf") {
    val e1 = intercept[AnalysisException](spark.conf.set("spark.driver.host", "myhost"))
    assert(e1.message.contains("https://spark.apache.org/docs/latest/sql-migration-guide.html"))
  }

  test("SPARK-21588 SQLContext.getConf(key, null) should return null") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      assert("1" == spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key, null))
      assert("1" == spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key, "<undefined>"))
    }

    assert(spark.conf.getOption("spark.sql.nonexistent").isEmpty)
    assert(null == spark.conf.get("spark.sql.nonexistent", null))
    assert("<undefined>" == spark.conf.get("spark.sql.nonexistent", "<undefined>"))
  }

  test("SPARK-10365: PARQUET_OUTPUT_TIMESTAMP_TYPE") {
    sqlConf.clear()

    // check default value
    assert(spark.sessionState.conf.parquetOutputTimestampType ==
      SQLConf.ParquetOutputTimestampType.INT96)

    sqlConf.setConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE, "timestamp_micros")
    assert(spark.sessionState.conf.parquetOutputTimestampType ==
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS)
    sqlConf.setConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE, "int96")
    assert(spark.sessionState.conf.parquetOutputTimestampType ==
      SQLConf.ParquetOutputTimestampType.INT96)

    // test invalid conf value
    intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "invalid")
    }

    sqlConf.clear()
  }

  test("SPARK-22779: correctly compute default value for fallback configs") {
    val fallback = SQLConf.buildConf("spark.sql.__test__.spark_22779")
      .fallbackConf(SQLConf.PARQUET_COMPRESSION)

    assert(spark.conf.get(fallback.key) ===
      SQLConf.PARQUET_COMPRESSION.defaultValue.get)
    assert(spark.conf.get(fallback.key, LZO.lowerCaseName()) === LZO.lowerCaseName())

    val displayValue = spark.sessionState.conf.getAllDefinedConfs
      .find { case (key, _, _, _) => key == fallback.key }
      .map { case (_, v, _, _) => v }
      .get
    assert(displayValue === fallback.defaultValueString)

    sqlConf.setConf(SQLConf.PARQUET_COMPRESSION, GZIP.lowerCaseName())
    assert(spark.conf.get(fallback.key) === GZIP.lowerCaseName())

    sqlConf.setConf(fallback, LZO.lowerCaseName())
    assert(spark.conf.get(fallback.key) === LZO.lowerCaseName())

    val newDisplayValue = spark.sessionState.conf.getAllDefinedConfs
      .find { case (key, _, _, _) => key == fallback.key }
      .map { case (_, v, _, _) => v }
      .get
    assert(newDisplayValue === LZO.lowerCaseName())

    SQLConf.unregister(fallback)
  }

  test("SPARK-24783: spark.sql.shuffle.partitions=0 should throw exception ") {
    val e = intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 0)
    }
    assert(e.getMessage.contains("spark.sql.shuffle.partitions"))
    val e2 = intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, -1)
    }
    assert(e2.getMessage.contains("spark.sql.shuffle.partitions"))
  }

  test("set removed config to non-default value") {
    val config = "spark.sql.fromJsonForceNullableSchema"
    val defaultValue = true

    spark.conf.set(config, defaultValue)

    val e = intercept[AnalysisException] {
      spark.conf.set(config, !defaultValue)
    }
    assert(e.getMessage.contains(config))
  }

  test("log deprecation warnings") {
    val logAppender = new LogAppender("deprecated SQL configs")
    def check(config: String): Unit = {
      assert(logAppender.loggingEvents.exists(
        e => e.getLevel == Level.WARN &&
        e.getMessage.getFormattedMessage.contains(config)))
    }

    val config1 = SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key
    withLogAppender(logAppender) {
      spark.conf.set(config1, 1)
    }
    check(config1)

    val config2 = SQLConf.ARROW_EXECUTION_ENABLED.key
    withLogAppender(logAppender) {
      spark.conf.unset(config2)
    }
    check(config2)
  }

  test("spark.sql.session.timeZone should only accept valid zone id") {
    spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, MIT.getId)
    assert(sql(s"set ${SQLConf.SESSION_LOCAL_TIMEZONE.key}").head().getString(1) === MIT.getId)
    spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Chicago")
    assert(sql(s"set ${SQLConf.SESSION_LOCAL_TIMEZONE.key}").head().getString(1) ===
      "America/Chicago")
    spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "GMT+8:00")
    assert(sql(s"set ${SQLConf.SESSION_LOCAL_TIMEZONE.key}").head().getString(1) === "GMT+8:00")

    intercept[SparkIllegalArgumentException] {
      spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "pst")
    }

    val invalidTz = "Asia/shanghai"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, invalidTz)
      },
      condition = "INVALID_CONF_VALUE.TIME_ZONE",
      parameters = Map(
        "confValue" -> invalidTz,
        "confName" -> SQLConf.SESSION_LOCAL_TIMEZONE.key))
  }

  test("set time zone") {
    TimeZone.getAvailableIDs().foreach { zid =>
      sql(s"set time zone '$zid'")
      assert(sqlConf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE) === zid)
    }
    sql("set time zone local")
    assert(sqlConf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE) === TimeZone.getDefault.getID)

    val tz = "Invalid TZ"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        sql(s"SET TIME ZONE '$tz'").collect()
      },
      condition = "INVALID_CONF_VALUE.TIME_ZONE",
      parameters = Map(
        "confValue" -> tz,
        "confName" -> SQLConf.SESSION_LOCAL_TIMEZONE.key))

    (-18 to 18).map(v => (v, s"interval '$v' hours")).foreach { case (i, interval) =>
      sql(s"set time zone $interval")
      val zone = sqlConf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE)
      if (i == 0) {
        assert(zone === "Z")
      } else {
        assert(zone === String.format("%+03d:00", Integer.valueOf(i)))
      }
    }
    val sqlText = "set time zone interval 19 hours"
    checkError(
      exception = intercept[ParseException](sql(sqlText)),
      condition = "_LEGACY_ERROR_TEMP_0044",
      parameters = Map.empty,
      context = ExpectedContext(sqlText, 0, 30))
  }

  test("SPARK-34454: configs from the legacy namespace should be internal") {
    val nonInternalLegacyConfigs = spark.sessionState.conf.getAllDefinedConfs
      .filter { case (key, _, _, _) => key.contains("spark.sql.legacy.") }
    assert(nonInternalLegacyConfigs.isEmpty,
      s"""
         |Non internal legacy SQL configs:
         |${nonInternalLegacyConfigs.map(_._1).mkString("\n")}
         |""".stripMargin)
  }

  test("SPARK-47765: set collation") {
    Seq("UNICODE", "UNICODE_CI", "utf8_lcase", "utf8_binary").foreach { collation =>
      sql(s"set collation $collation")
      assert(sqlConf.getConf(SQLConf.DEFAULT_COLLATION) === collation.toUpperCase(Locale.ROOT))
    }

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        sql(s"SET COLLATION unicode_c").collect()
      },
      condition = "INVALID_CONF_VALUE.DEFAULT_COLLATION",
      parameters = Map(
        "confValue" -> "UNICODE_C",
        "confName" -> "spark.sql.session.collation.default",
        "proposals" -> "UNICODE"
      ))

    withSQLConf(SQLConf.TRIM_COLLATION_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException](sql(s"SET COLLATION UNICODE_CI_RTRIM")),
        condition = "UNSUPPORTED_FEATURE.TRIM_COLLATION"
      )
    }
  }

  test("SPARK-43028: config not found error") {
    checkError(
      exception = intercept[SparkNoSuchElementException](spark.conf.get("some.conf")),
      condition = "SQL_CONF_NOT_FOUND",
      parameters = Map("sqlConf" -> "\"some.conf\""))
  }
}
