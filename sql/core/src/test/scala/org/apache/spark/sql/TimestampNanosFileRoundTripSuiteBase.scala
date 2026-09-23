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

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Parquet / ORC write->read round-trips over the nanosecond timestamp types (`TIMESTAMP_NTZ(p)` /
 * `TIMESTAMP_LTZ(p)`, `p` in `[7, 9]`), checking the sub-microsecond remainder, precision flooring
 * (p=7/p=8) and NULLs all survive. The two subclasses run ANSI on and off.
 */
abstract class TimestampNanosFileRoundTripSuiteBase extends SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  private val formats = Seq("parquet", "orc")

  private def nanos(label: String, p: Int): DataType =
    if (label == "NTZ") TimestampNTZNanosType(p) else TimestampLTZNanosType(p)

  // One column per precision plus a NULL; `.123456789` floors at p=7/p=8, `.000000999` keeps p=9.
  private def probeDf(typ: String, zone: String): DataFrame = spark.sql(
    s"""SELECT
       |  '2020-01-01 00:00:00.123456789$zone' :: $typ(7) AS c7,
       |  '2020-01-01 00:00:00.123456789$zone' :: $typ(8) AS c8,
       |  '2020-01-01 00:00:00.000000999$zone' :: $typ(9) AS c9,
       |  CAST(NULL AS $typ(9)) AS cnull""".stripMargin)

  Seq(("NTZ", "timestamp_ntz", ""), ("LTZ", "timestamp_ltz", " UTC")).foreach {
    case (label, typ, zone) =>
      formats.foreach { fmt =>
        test(s"$label: $fmt round-trips nanosecond timestamps (sub-microsecond, flooring, NULL)") {
          withTempPath { dir =>
            val path = dir.getCanonicalPath
            val df = probeDf(typ, zone)
            val schema = df.schema
            assert(schema("c7").dataType === nanos(label, 7))
            assert(schema("c9").dataType === nanos(label, 9))
            df.write.mode("overwrite").format(fmt).save(path)
            val read = spark.read.schema(schema).format(fmt).load(path)
            assert(read.schema === schema)
            checkAnswer(read, df.collect().toSeq)
          }
        }
      }
  }
}

class TimestampNanosFileRoundTripAnsiOnSuite extends TimestampNanosFileRoundTripSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

class TimestampNanosFileRoundTripAnsiOffSuite extends TimestampNanosFileRoundTripSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
