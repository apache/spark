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

package org.apache.spark.sql.pipelines.util

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.internal.config.{AUTH_SECRET, BUFFER_SIZE, DEFAULT_PARALLELISM, DRIVER_MEMORY, UNSAFE_EXCEPTION_ON_MEMORY_LEAK}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class SparkSessionUtilsSuite extends QueryTest with SharedSparkSession {

  private val helperLoggerName = SparkSessionUtils.getClass.getName.stripSuffix("$")

  /**
   * Runs `withSqlConf` with `pairs` around `f` and returns the result of `f` together with the
   * warnings the helper logged.
   */
  private def withSkipWarnings[T](pairs: (String, String)*)(f: => T): (T, Seq[String]) = {
    val appender = new LogAppender("withSqlConf skip warnings")
    var result: Option[T] = None
    withLogAppender(appender, Seq(helperLoggerName)) {
      result = Some(SparkSessionUtils.withSqlConf(spark, pairs: _*)(f))
    }
    val warnings = appender.loggingEvents
      .filter(_.getLevel == Level.WARN)
      .map(_.getMessage.getFormattedMessage)
      .toSeq
    (result.get, warnings)
  }

  test("withSqlConf sets the confs for the duration of the block and restores them") {
    val setKey = SQLConf.SHUFFLE_PARTITIONS.key
    val unsetKey = "spark.sql.pipelines.test.unsetConf"
    val original = spark.conf.get(setKey)
    val inside = SparkSessionUtils.withSqlConf(spark, setKey -> "7", unsetKey -> "x") {
      (spark.conf.get(setKey), spark.conf.get(unsetKey))
    }
    assert(inside == ("7", "x"))
    assert(spark.conf.get(setKey) == original)
    assert(spark.conf.getOption(unsetKey).isEmpty)
  }

  test("withSqlConf skips confs that cannot be modified on a running session") {
    val staticKey = StaticSQLConf.CATALOG_IMPLEMENTATION.key
    val staticValue = spark.conf.get(staticKey)
    val otherStaticValue = if (staticValue == "hive") "in-memory" else "hive"
    val coreKey = DRIVER_MEMORY.key
    val coreValue = spark.conf.getOption(coreKey)
    Seq(staticValue, otherStaticValue).foreach { v =>
      val inside = SparkSessionUtils.withSqlConf(
        spark,
        staticKey -> v,
        coreKey -> "1234m",
        SQLConf.SHUFFLE_PARTITIONS.key -> "7") {
        (spark.conf.get(staticKey), spark.conf.getOption(coreKey),
          spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key))
      }
      // Confs that cannot be modified keep the session's value; others are still applied.
      assert(inside == ((staticValue, coreValue, "7")))
    }
    assert(spark.conf.get(staticKey) == staticValue)
    assert(spark.conf.getOption(coreKey) == coreValue)
  }

  test("SPARK-61825: core confs are applied when the session does not reject them") {
    val coreKey = BUFFER_SIZE.key
    val original = spark.conf.getOption(coreKey)
    assert(!original.contains("4096"))
    withSQLConf(SQLConf.SET_COMMAND_REJECTS_SPARK_CORE_CONFS.key -> "false") {
      val (inside, warnings) = withSkipWarnings(coreKey -> "4096")(spark.conf.get(coreKey))
      assert(inside == "4096")
      assert(warnings.isEmpty, warnings)
      assert(spark.conf.getOption(coreKey) == original)
    }
  }

  test("SPARK-61825: spark.default.parallelism is applied and restored") {
    val key = DEFAULT_PARALLELISM.key
    val original = spark.conf.getOption(key)
    assert(!original.contains("3"))
    val (inside, warnings) = withSkipWarnings(key -> "3")(spark.conf.get(key))
    assert(inside == "3")
    assert(warnings.isEmpty, warnings)
    assert(spark.conf.getOption(key) == original)
  }

  test("SPARK-61825: a skip warning names the key only when the requested value differs") {
    val staticKey = StaticSQLConf.CATALOG_IMPLEMENTATION.key
    val staticValue = spark.conf.get(staticKey)
    val otherStaticValue = if (staticValue == "hive") "in-memory" else "hive"
    // A registered Spark core key that the test session's SparkConf sets.
    val coreKey = UNSAFE_EXCEPTION_ON_MEMORY_LEAK.key
    val coreValue = spark.conf.get(coreKey)
    val otherCoreValue = (!coreValue.toBoolean).toString
    Seq(
      (staticKey, staticValue, otherStaticValue),
      (coreKey, coreValue, otherCoreValue)).foreach { case (key, sameValue, otherValue) =>
      val (_, noWarnings) = withSkipWarnings(key -> sameValue)(())
      assert(noWarnings.isEmpty, noWarnings)
      val (_, warnings) = withSkipWarnings(key -> otherValue)(())
      assert(warnings.size == 1, warnings)
      assert(warnings.head.contains(key))
    }
  }

  test("SPARK-61825: the skip warning redacts a sensitive requested value") {
    val key = AUTH_SECRET.key
    val requested = "hunter2-value"
    assert(!spark.conf.getOption(key).contains(requested))
    val (_, warnings) = withSkipWarnings(key -> requested)(())
    assert(warnings.size == 1, warnings)
    assert(warnings.head.contains(key))
    assert(!warnings.head.contains(requested), warnings.head)
  }

  test("SPARK-61825: the skip warning shows a requested value that is not sensitive") {
    val key = StaticSQLConf.CATALOG_IMPLEMENTATION.key
    val otherValue = if (spark.conf.get(key) == "hive") "in-memory" else "hive"
    val (_, warnings) = withSkipWarnings(key -> otherValue)(())
    assert(warnings.size == 1, warnings)
    assert(warnings.head.contains(s"$key=$otherValue"), warnings.head)
  }

  test("SPARK-61825: no skip warning for another spelling of the session's warehouse directory") {
    val key = StaticSQLConf.WAREHOUSE_PATH.key
    val sessionValue = spark.conf.get(key)
    val localPath = new Path(sessionValue).toUri.getPath
    Seq(localPath, s"file://$localPath").foreach { spelling =>
      assert(spelling != sessionValue)
      val (_, warnings) = withSkipWarnings(key -> spelling)(())
      assert(warnings.isEmpty, s"$spelling: $warnings")
    }
  }

  test("SPARK-61825: a skip warning for a warehouse directory that differs from the session's") {
    val key = StaticSQLConf.WAREHOUSE_PATH.key
    withTempDir { dir =>
      val (_, warnings) = withSkipWarnings(key -> dir.getCanonicalPath)(())
      assert(warnings.size == 1, warnings)
      assert(warnings.head.contains(key))
    }
  }
}
