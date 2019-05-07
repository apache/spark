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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.test.{SharedSQLContext, TestSQLContext}
import org.apache.spark.util.Utils

class SQLConfSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

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
      sql(s"set ${SQLConf.SHUFFLE_PARTITIONS}=$original")
    }
  }

  test("reset - public conf") {
    spark.sessionState.conf.clear()
    val original = spark.conf.get(SQLConf.GROUP_BY_ORDINAL)
    try {
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL))
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL.key}=false")
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL) === false)
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 1)
      sql(s"reset")
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL))
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 0)
    } finally {
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL}=$original")
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
      sql(s"set ${SQLConf.OPTIMIZER_MAX_ITERATIONS}=$original")
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

  test("invalid conf value") {
    spark.sessionState.conf.clear()
    val e = intercept[IllegalArgumentException] {
      sql(s"set ${SQLConf.CASE_SENSITIVE.key}=10")
    }
    assert(e.getMessage === s"${SQLConf.CASE_SENSITIVE.key} should be boolean, but was 10")
  }

  test("Test SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE's method") {
    spark.sessionState.conf.clear()

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "100")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 100)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "1k")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 1024)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "1M")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 1048576)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "1g")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 1073741824)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "-1")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === -1)

    // Test overflow exception
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MaxValue
      spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "90000000000g")
    }

    intercept[IllegalArgumentException] {
      // This value less than Long.MinValue
      spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "-90000000000g")
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
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS)

    // PARQUET_INT64_AS_TIMESTAMP_MILLIS should be respected.
    spark.sessionState.conf.setConf(SQLConf.PARQUET_INT64_AS_TIMESTAMP_MILLIS, true)
    assert(spark.sessionState.conf.parquetOutputTimestampType ==
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS)

    // PARQUET_OUTPUT_TIMESTAMP_TYPE has higher priority over PARQUET_INT64_AS_TIMESTAMP_MILLIS
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
      .find { case (key, _, _) => key == fallback.key }
      .map { case (_, v, _) => v }
      .get
    assert(displayValue === fallback.defaultValueString)

    spark.sessionState.conf.setConf(SQLConf.PARQUET_COMPRESSION, "gzip")
    assert(spark.sessionState.conf.getConfString(fallback.key) === "gzip")

    spark.sessionState.conf.setConf(fallback, "lzo")
    assert(spark.sessionState.conf.getConfString(fallback.key) === "lzo")

    val newDisplayValue = spark.sessionState.conf.getAllDefinedConfs
      .find { case (key, _, _) => key == fallback.key }
      .map { case (_, v, _) => v }
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

}
