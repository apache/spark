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

import java.util.TimeZone

import org.apache.hadoop.fs.Path
import org.apache.log4j.Level

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.MIT
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
    spark.sessionState.conf.clear()
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

    spark.sessionState.conf.clear()
  }

  test("parse SQL set commands") {
    spark.sessionState.conf.clear()
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

    spark.sessionState.conf.clear()
  }

  test("set command for display") {
    spark.sessionState.conf.clear()
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
    spark.sessionState.conf.clear()
    val original = spark.conf.get(SQLConf.SHUFFLE_PARTITIONS)
    try {
      sql(s"set ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS}=10")
      assert(spark.conf.get(SQLConf.SHUFFLE_PARTITIONS) === 10)
    } finally {
      sql(s"set ${SQLConf.SHUFFLE_PARTITIONS.key}=$original")
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
    spark.sessionState.conf.clear()
    val original = spark.conf.get(SQLConf.GROUP_BY_ORDINAL)
    try {
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL))
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL.key}=false")
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL) === false)
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 1)
      assert(spark.conf.get(SQLConf.OPTIMIZER_EXCLUDED_RULES).isEmpty)
      sql(s"reset")
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL))
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 0)
      assert(spark.conf.get(SQLConf.OPTIMIZER_EXCLUDED_RULES) ===
        Some("org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"))
    } finally {
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL.key}=$original")
    }
  }

  test("reset - internal conf") {
    spark.sessionState.conf.clear()
    val original = spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS)
    try {
      assert(spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 100)
      sql(s"set ${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}=10")
      assert(spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 10)
      assert(sql(s"set").where(s"key = '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}'").count() == 1)
      sql(s"reset")
      assert(spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 100)
      assert(sql(s"set").where(s"key = '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}'").count() == 0)
    } finally {
      sql(s"set ${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}=$original")
    }
  }

  test("reset - user-defined conf") {
    spark.sessionState.conf.clear()
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
    spark.sessionState.conf.clear()
    // spark core conf w/o entry registered
    val appId = spark.sparkContext.getConf.getAppId
    sql("RESET spark.app.id")
    assert(spark.conf.get("spark.app.id") === appId, "Should not change spark core ones")
    // spark core conf w/ entry registered
    val e1 = intercept[AnalysisException](sql("RESET spark.executor.cores"))
    assert(e1.getMessage === "Cannot modify the value of a Spark config: spark.executor.cores")

    // user defined settings
    sql("SET spark.abc=xyz")
    assert(spark.conf.get("spark.abc") === "xyz")
    sql("RESET spark.abc")
    intercept[NoSuchElementException](spark.conf.get("spark.abc"))
    sql("RESET spark.abc") // ignore nonexistent keys

    // runtime sql configs
    val original = spark.conf.get(SQLConf.GROUP_BY_ORDINAL)
    sql(s"SET ${SQLConf.GROUP_BY_ORDINAL.key}=false")
    sql(s"RESET ${SQLConf.GROUP_BY_ORDINAL.key}")
    assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL) === original)

    // runtime sql configs with optional defaults
    assert(spark.conf.get(SQLConf.OPTIMIZER_EXCLUDED_RULES).isEmpty)
    sql(s"RESET ${SQLConf.OPTIMIZER_EXCLUDED_RULES.key}")
    assert(spark.conf.get(SQLConf.OPTIMIZER_EXCLUDED_RULES) ===
      Some("org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"))
    sql(s"SET ${SQLConf.PLAN_CHANGE_LOG_RULES.key}=abc")
    sql(s"RESET ${SQLConf.PLAN_CHANGE_LOG_RULES.key}")
    assert(spark.conf.get(SQLConf.PLAN_CHANGE_LOG_RULES).isEmpty)

    // static sql configs
    val e2 = intercept[AnalysisException](sql(s"RESET ${StaticSQLConf.WAREHOUSE_PATH.key}"))
    assert(e2.getMessage ===
      s"Cannot modify the value of a static config: ${StaticSQLConf.WAREHOUSE_PATH.key}")

  }

  test("invalid conf value") {
    spark.sessionState.conf.clear()
    val e = intercept[IllegalArgumentException] {
      sql(s"set ${SQLConf.CASE_SENSITIVE.key}=10")
    }
    assert(e.getMessage === s"${SQLConf.CASE_SENSITIVE.key} should be boolean, but was 10")
  }

  test("Test ADVISORY_PARTITION_SIZE_IN_BYTES's method") {
    spark.sessionState.conf.clear()

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "100")
    assert(spark.conf.get(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 100)

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "1k")
    assert(spark.conf.get(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 1024)

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "1M")
    assert(spark.conf.get(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 1048576)

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "1g")
    assert(spark.conf.get(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === 1073741824)

    spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "-1")
    assert(spark.conf.get(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES) === -1)

    // Test overflow exception
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MaxValue
      spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "90000000000g")
    }

    intercept[IllegalArgumentException] {
      // This value less than Long.MinValue
      spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "-90000000000g")
    }

    spark.sessionState.conf.clear()
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
    val previousValue = sparkContext.conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
    try {
      sparkContext.conf.set(SCHEMA_STRING_LENGTH_THRESHOLD, 2000)
      val newSession = new SparkSession(sparkContext)
      assert(newSession.conf.get(SCHEMA_STRING_LENGTH_THRESHOLD) == 2000)
      checkAnswer(
        newSession.sql(s"SET ${SCHEMA_STRING_LENGTH_THRESHOLD.key}"),
        Row(SCHEMA_STRING_LENGTH_THRESHOLD.key, "2000"))
    } finally {
      sparkContext.conf.set(SCHEMA_STRING_LENGTH_THRESHOLD, previousValue)
    }
  }

  test("cannot set/unset static SQL conf") {
    val e1 = intercept[AnalysisException](sql(s"SET ${SCHEMA_STRING_LENGTH_THRESHOLD.key}=10"))
    assert(e1.message.contains("Cannot modify the value of a static config"))
    val e2 = intercept[AnalysisException](spark.conf.unset(SCHEMA_STRING_LENGTH_THRESHOLD.key))
    assert(e2.message.contains("Cannot modify the value of a static config"))
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
    spark.sessionState.conf.clear()

    // check default value
    assert(spark.sessionState.conf.parquetOutputTimestampType ==
      SQLConf.ParquetOutputTimestampType.INT96)

    spark.sessionState.conf.setConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE, "timestamp_micros")
    assert(spark.sessionState.conf.parquetOutputTimestampType ==
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS)
    spark.sessionState.conf.setConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE, "int96")
    assert(spark.sessionState.conf.parquetOutputTimestampType ==
      SQLConf.ParquetOutputTimestampType.INT96)

    // test invalid conf value
    intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "invalid")
    }

    spark.sessionState.conf.clear()
  }

  test("SPARK-22779: correctly compute default value for fallback configs") {
    val fallback = SQLConf.buildConf("spark.sql.__test__.spark_22779")
      .fallbackConf(SQLConf.PARQUET_COMPRESSION)

    assert(spark.sessionState.conf.getConfString(fallback.key) ===
      SQLConf.PARQUET_COMPRESSION.defaultValue.get)
    assert(spark.sessionState.conf.getConfString(fallback.key, "lzo") === "lzo")

    val displayValue = spark.sessionState.conf.getAllDefinedConfs
      .find { case (key, _, _, _) => key == fallback.key }
      .map { case (_, v, _, _) => v }
      .get
    assert(displayValue === fallback.defaultValueString)

    spark.sessionState.conf.setConf(SQLConf.PARQUET_COMPRESSION, "gzip")
    assert(spark.sessionState.conf.getConfString(fallback.key) === "gzip")

    spark.sessionState.conf.setConf(fallback, "lzo")
    assert(spark.sessionState.conf.getConfString(fallback.key) === "lzo")

    val newDisplayValue = spark.sessionState.conf.getAllDefinedConfs
      .find { case (key, _, _, _) => key == fallback.key }
      .map { case (_, v, _, _) => v }
      .get
    assert(newDisplayValue === "lzo")

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
        e.getRenderedMessage.contains(config)))
    }

    val config1 = SQLConf.HIVE_VERIFY_PARTITION_PATH.key
    withLogAppender(logAppender) {
      spark.conf.set(config1, true)
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

    intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "pst")
    }
    intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "GMT+8:00")
    }
    val e = intercept[IllegalArgumentException] {
      spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "Asia/shanghai")
    }
    assert(e.getMessage === "Cannot resolve the given timezone with ZoneId.of(_, ZoneId.SHORT_IDS)")
  }

  test("set time zone") {
    TimeZone.getAvailableIDs().foreach { zid =>
      sql(s"set time zone '$zid'")
      assert(spark.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE) === zid)
    }
    sql("set time zone local")
    assert(spark.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE) === TimeZone.getDefault.getID)

    val e1 = intercept[IllegalArgumentException](sql("set time zone 'invalid'"))
    assert(e1.getMessage === "Cannot resolve the given timezone with" +
      " ZoneId.of(_, ZoneId.SHORT_IDS)")

    (-18 to 18).map(v => (v, s"interval '$v' hours")).foreach { case (i, interval) =>
      sql(s"set time zone $interval")
      val zone = spark.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
      if (i == 0) {
        assert(zone === "Z")
      } else {
        assert(zone === String.format("%+03d:00", new Integer(i)))
      }
    }
    val e2 = intercept[ParseException](sql("set time zone interval 19 hours"))
    assert(e2.getMessage contains "The interval value must be in the range of [-18, +18] hours")
  }
}
