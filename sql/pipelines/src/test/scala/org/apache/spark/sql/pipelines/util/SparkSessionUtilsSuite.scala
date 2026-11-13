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

import org.apache.spark.internal.config.DRIVER_MEMORY
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class SparkSessionUtilsSuite extends QueryTest with SharedSparkSession {

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
}
