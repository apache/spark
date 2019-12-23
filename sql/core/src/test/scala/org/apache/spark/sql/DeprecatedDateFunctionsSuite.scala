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

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

@deprecated("This test suite will be removed.", "3.0.0")
class DeprecatedDateFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("from_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
      checkAnswer(
        df.select(from_utc_timestamp(col("a"), "PST")),
        Seq(
          Row(Timestamp.valueOf("2015-07-23 17:00:00")),
          Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
      checkAnswer(
        df.select(from_utc_timestamp(col("b"), "PST")),
        Seq(
          Row(Timestamp.valueOf("2015-07-23 17:00:00")),
          Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
    }
    val msg = intercept[AnalysisException] {
      df.select(from_utc_timestamp(col("a"), "PST")).collect()
    }.getMessage
    assert(msg.contains(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key))
  }

  test("from_utc_timestamp with column zone") {
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
      val df = Seq(
        (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00", "CET"),
        (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00", "PST")
      ).toDF("a", "b", "c")
      checkAnswer(
        df.select(from_utc_timestamp(col("a"), col("c"))),
        Seq(
          Row(Timestamp.valueOf("2015-07-24 02:00:00")),
          Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
      checkAnswer(
        df.select(from_utc_timestamp(col("b"), col("c"))),
        Seq(
          Row(Timestamp.valueOf("2015-07-24 02:00:00")),
          Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
    }
  }

  test("to_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
      checkAnswer(
        df.select(to_utc_timestamp(col("a"), "PST")),
        Seq(
          Row(Timestamp.valueOf("2015-07-24 07:00:00")),
          Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
      checkAnswer(
        df.select(to_utc_timestamp(col("b"), "PST")),
        Seq(
          Row(Timestamp.valueOf("2015-07-24 07:00:00")),
          Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
    }
    val msg = intercept[AnalysisException] {
      df.select(to_utc_timestamp(col("a"), "PST")).collect()
    }.getMessage
    assert(msg.contains(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key))
  }

  test("to_utc_timestamp with column zone") {
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
      val df = Seq(
        (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00", "PST"),
        (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00", "CET")
      ).toDF("a", "b", "c")
      checkAnswer(
        df.select(to_utc_timestamp(col("a"), col("c"))),
        Seq(
          Row(Timestamp.valueOf("2015-07-24 07:00:00")),
          Row(Timestamp.valueOf("2015-07-24 22:00:00"))))
      checkAnswer(
        df.select(to_utc_timestamp(col("b"), col("c"))),
        Seq(
          Row(Timestamp.valueOf("2015-07-24 07:00:00")),
          Row(Timestamp.valueOf("2015-07-24 22:00:00"))))
    }
  }
}
