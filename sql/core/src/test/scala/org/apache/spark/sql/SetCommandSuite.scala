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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSQLContext}
import org.apache.spark.util.ResetSystemProperties

class SetCommandSuite extends QueryTest with SharedSparkSession with ResetSystemProperties  {
  test("SET commands semantics using sql()") {
    spark.sessionState.conf.clear()
    val testKey = "test.key.0"
    val testVal = "test.val.0"
    val nonexistentKey = "nonexistent"

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(sql("SET").collect().length === TestSQLContext.overrideConfs.size)
    sql("SET").collect().foreach { row =>
      val key = row.getString(0)
      val value = row.getString(1)
      assert(
        TestSQLContext.overrideConfs.contains(key),
        s"$key should exist in SQLConf.")
      assert(
        TestSQLContext.overrideConfs(key) === value,
        s"The value of $key should be ${TestSQLContext.overrideConfs(key)} instead of $value.")
    }
    val overrideConfs = sql("SET").collect()

    // "set key=val"
    sql(s"SET $testKey=$testVal")
    checkAnswer(
      sql("SET"),
      overrideConfs ++ Seq(Row(testKey, testVal))
    )

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    checkAnswer(
      sql("set"),
      overrideConfs ++ Seq(Row(testKey, testVal), Row(testKey + testKey, testVal + testVal))
    )

    // "set key"
    checkAnswer(
      sql(s"SET $testKey"),
      Row(testKey, testVal)
    )
    checkAnswer(
      sql(s"SET $nonexistentKey"),
      Row(nonexistentKey, "<undefined>")
    )
    spark.sessionState.conf.clear()
  }

  test("SPARK-19218 SET command should show a result in a sorted order") {
    val overrideConfs = sql("SET").collect()
    sql(s"SET test.key3=1")
    sql(s"SET test.key2=2")
    sql(s"SET test.key1=3")
    val result = sql("SET").collect()
    assert(result ===
      (overrideConfs ++ Seq(
        Row("test.key1", "3"),
        Row("test.key2", "2"),
        Row("test.key3", "1"))).sortBy(_.getString(0))
    )
    spark.sessionState.conf.clear()
  }

  test("SET commands with illegal or inappropriate argument") {
    spark.sessionState.conf.clear()
    // Set negative mapred.reduce.tasks for automatically determining
    // the number of reducers is not supported
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-1"))
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-01"))
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-2"))
    spark.sessionState.conf.clear()
  }

  test("SET mapreduce.job.reduces automatically converted to spark.sql.shuffle.partitions") {
    spark.sessionState.conf.clear()
    val before = spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).toInt
    val newConf = before + 1
    sql(s"SET mapreduce.job.reduces=${newConf.toString}")
    val after = spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).toInt
    assert(before != after)
    assert(newConf === after)
    intercept[IllegalArgumentException](sql(s"SET mapreduce.job.reduces=-1"))
    spark.sessionState.conf.clear()
  }

  test("SPARK-35044: SET command shall display default value for hadoop conf correctly") {
    val key = "hadoop.this.is.a.test.key"
    val value = "2018-11-17 13:33:33.333"
    // these keys are located at `src/test/resources/hive-site.xml`
    checkAnswer(sql(s"SET $key"), Row(key, value))
    checkAnswer(sql("SET hadoop.tmp.dir"), Row("hadoop.tmp.dir", "/tmp/hive_one"))

    // these keys does not exist as default yet
    checkAnswer(sql(s"SET ${key}no"), Row(key + "no", "<undefined>"))
    checkAnswer(sql("SET dfs.hosts"), Row("dfs.hosts", "<undefined>"))

    // io.file.buffer.size has a default value from `SparkHadoopUtil.newConfiguration`
    checkAnswer(sql("SET io.file.buffer.size"), Row("io.file.buffer.size", "65536"))
  }

  test("SPARK-35576: Set command should redact sensitive data") {
    val key1 = "test.password"
    val value1 = "test.value1"
    val key2 = "test.token"
    val value2 = "test.value2"
    withSQLConf (key1 -> value1, key2 -> value2) {
      checkAnswer(sql(s"SET $key1"), Row(key1, "*********(redacted)"))
      checkAnswer(sql(s"SET $key2"), Row(key2, "*********(redacted)"))
      val allValues = sql("SET").collect().map(_.getString(1))
      assert(!allValues.exists(v => v.contains(value1) || v.contains(value2)))
    }
  }

  test("SPARK-42946: Set command could expose sensitive data through key") {
    val key1 = "test.password"
    val value1 = "test.value1"
    withSQLConf(key1 -> value1) {
      checkError(
        intercept[ParseException](sql("SET ${test.password}")),
        condition = "INVALID_SET_SYNTAX"
      )
    }
  }
}
