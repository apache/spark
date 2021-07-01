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
import java.time.LocalDateTime

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StructField, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.util.ResetSystemProperties

// Test suite for setting the default timestamp type of Spark SQL
class TimestampTypeSuite extends QueryTest with SharedSparkSession with ResetSystemProperties {
  test("Create and Alter Table") {
    Seq(("TIMESTAMP_NTZ", TimestampNTZType), ("TIMESTAMP_LTZ", TimestampType)).foreach {
      case (v, dt) =>
        withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> v) {
          withTable("t") {
            sql("create table t(ts timestamp) using csv")
            val expectedSchema = new StructType().add(StructField("ts", dt))
            assert(spark.table("t").schema == expectedSchema)
            sql("alter table t add column (ts2 timestamp)")
            val expectedSchema2 = expectedSchema.add(StructField("ts2", dt))
            assert(spark.table("t").schema == expectedSchema2)
          }
        }
    }
  }

  test("Cast") {
    val timestampStr = "2021-01-01 00:00:00"
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_NTZ") {
      val df = sql(s"select cast('$timestampStr' as timestamp) as ts")
      val expectedSchema = new StructType().add(StructField("ts", TimestampNTZType))
      assert(df.schema == expectedSchema)
      val expectedAnswer = Row(LocalDateTime.parse(timestampStr.replace(" ", "T")))
      checkAnswer(df, expectedAnswer)
    }
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_LTZ") {
      val df = sql(s"select cast('$timestampStr' as timestamp) as ts")
      val expectedSchema = new StructType().add(StructField("ts", TimestampType))
      assert(df.schema == expectedSchema)
      val expectedAnswer = Row(Timestamp.valueOf(timestampStr))
      checkAnswer(df, expectedAnswer)
    }
  }
}
