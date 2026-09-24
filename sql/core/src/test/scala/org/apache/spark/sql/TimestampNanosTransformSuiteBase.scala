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

import org.apache.spark.{SparkConf, TestUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Script TRANSFORM with a nanosecond timestamp output column: the value is written to the script
 * at full precision and parsed back into the declared `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` type
 * (SPARK-56822). The two subclasses run ANSI on and off.
 */
abstract class TimestampNanosTransformSuiteBase extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  test("TRANSFORM output as TIMESTAMP_NTZ(p) round-trips the sub-microsecond value") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    val df = spark.sql(
      """SELECT TRANSFORM(c7, c8, c9)
        |  USING 'cat' AS (c7 TIMESTAMP_NTZ(7), c8 TIMESTAMP_NTZ(8), c9 TIMESTAMP_NTZ(9))
        |FROM VALUES (TIMESTAMP_NTZ '2020-01-01 00:00:00.1234567',
        |             TIMESTAMP_NTZ '2020-01-01 00:00:00.12345678',
        |             TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789') t(c7, c8, c9)""".stripMargin)
    assert(df.schema("c7").dataType === TimestampNTZNanosType(7))
    assert(df.schema("c8").dataType === TimestampNTZNanosType(8))
    assert(df.schema("c9").dataType === TimestampNTZNanosType(9))
    checkAnswer(df, spark.sql(
      "SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.1234567', " +
        "TIMESTAMP_NTZ '2020-01-01 00:00:00.12345678', " +
        "TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789'").collect().head)
  }

  test("TRANSFORM output as TIMESTAMP_LTZ(p) round-trips the sub-microsecond value") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    val df = spark.sql(
      """SELECT TRANSFORM(c) USING 'cat' AS (c TIMESTAMP_LTZ(9))
        |FROM VALUES (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000999 UTC') t(c)""".stripMargin)
    assert(df.schema("c").dataType === TimestampLTZNanosType(9))
    checkAnswer(df, spark.sql(
      "SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000999 UTC'").collect().head)
  }
}

class TimestampNanosTransformAnsiOnSuite extends TimestampNanosTransformSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

class TimestampNanosTransformAnsiOffSuite extends TimestampNanosTransformSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
