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
 * End-to-end file write/read round-trip tests over the nanosecond-precision timestamp types
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`), part of the nanosecond timestamp
 * preview (SPARK-56822). A DataFrame carrying nanos columns is written to Parquet and to ORC and
 * read back, asserting that the SUB-MICROSECOND remainder survives the round trip, that a value
 * below the type's precision stays floored to the type's grid (p=7 -> 100 ns, p=8 -> 10 ns), and
 * that NULLs survive -- a micro-truncating writer or reader would silently drop the sub-microsecond
 * half.
 *
 * Scope note: the Parquet read/converter internals are unit-tested in
 * `TimestampNanosParquetOpsSuite` and an ORC read-then-sort path is exercised in
 * `TimestampNanosSortSuiteBase`; this suite adds the end-to-end DataFrame value-preservation
 * round trip (both formats, both time-zone families, precision flooring and NULL), which neither
 * of those covers.
 *
 * The nanosecond timestamp types are gated behind a preview flag enabled by default under tests
 * (`Utils.isTesting`), so it is not set here. The session time zone is fixed so `TIMESTAMP_LTZ`
 * values are deterministic. The two subclasses run every test with ANSI mode on and off.
 */
abstract class TimestampNanosFileRoundTripSuiteBase extends SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  private val formats = Seq("parquet", "orc")

  // A single-row probe frame with a column at each precision plus a NULL. `.123456789` floors to
  // `.1234567` at p=7 and `.12345678` at p=8; `.000000999` keeps its sub-microsecond digit at p=9.
  // LTZ literals carry an explicit UTC zone so the instant is session-zone independent.
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
            // The declared schema must carry each precision through unchanged.
            assert(schema("c7").dataType === (if (label == "NTZ") TimestampNTZNanosType(7)
              else TimestampLTZNanosType(7)))
            assert(schema("c9").dataType === (if (label == "NTZ") TimestampNTZNanosType(9)
              else TimestampLTZNanosType(9)))

            df.write.mode("overwrite").format(fmt).save(path)
            val read = spark.read.schema(schema).format(fmt).load(path)
            assert(read.schema === schema)
            // checkAnswer is value-based; equal iff every column (incl. the sub-microsecond
            // remainder, the floored p=7/p=8 fractions and the NULL) round-tripped intact.
            checkAnswer(read, df.collect().toSeq)
          }
        }
      }
  }
}

// Runs the nanosecond timestamp file round-trip tests with ANSI mode enabled explicitly.
class TimestampNanosFileRoundTripAnsiOnSuite extends TimestampNanosFileRoundTripSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp file round-trip tests with ANSI mode disabled explicitly.
class TimestampNanosFileRoundTripAnsiOffSuite extends TimestampNanosFileRoundTripSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
