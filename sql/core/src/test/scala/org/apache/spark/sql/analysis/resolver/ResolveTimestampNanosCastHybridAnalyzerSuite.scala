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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end checks that `DATE <-> nanos(p)` casts resolve under the single-pass analyzer the same
 * way they do under the fixed-point analyzer.
 *
 * `DATE <-> nanos(p)` is not directly castable, so the fixed-point `ResolveTimestampNanosCast` rule
 * rewrites it through the microsecond timestamp type. Without an equivalent rewrite in the
 * single-pass resolver, the cast fails its input type check and dual-run mode throws
 * `HYBRID_ANALYZER_EXCEPTION.SINGLE_PASS_FAILED_FIXED_POINT_SUCCEEDED`. Each query below is
 * analyzed under dual-run mode (which compares the single-pass and fixed-point plans and fails on
 * any divergence) and its result is compared against the fixed-point-only result.
 */
class ResolveTimestampNanosCastHybridAnalyzerSuite extends QueryTest with SharedSparkSession {

  private def checkDualRunMatchesFixedPoint(query: String): Unit = {
    val expected = withSQLConf(
      SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
      SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false"
    ) {
      sql(query).collect().toIndexedSeq
    }

    withSQLConf(
      SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
      SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "true",
      SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE.key -> "1.0",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE.key -> "true"
    ) {
      // sql() analyzes eagerly, so the dual-run comparison happens here.
      checkAnswer(sql(query), expected)
    }
  }

  test("SPARK-57323: DATE <-> TIMESTAMP_NTZ(p) casts resolve under the single-pass analyzer") {
    Seq(
      // Nanos-typed outputs are rendered via ::string so they materialize independently of the
      // nanosecond type's collection support; the DATE -> nanos cast still drives the rewrite.
      "SELECT (DATE '2020-01-01'::timestamp_ntz(9))::string",
      "SELECT (DATE '2020-01-01'::timestamp_ntz(7))::string",
      "SELECT TIMESTAMP_NTZ '2020-01-01 12:30:15.123456789'::date",
      "SELECT TIMESTAMP_NTZ '1960-01-01 00:00:00.000000001'::date",
      "SELECT DATE '2020-01-01'::timestamp_ntz(9)::date",
      "SELECT ((NULL::date)::timestamp_ntz(9))::string",
      "SELECT (NULL::timestamp_ntz(9))::date"
    ).foreach(checkDualRunMatchesFixedPoint)
  }

  test("SPARK-57323: DATE <-> TIMESTAMP_LTZ(p) casts resolve under the single-pass analyzer") {
    Seq(
      "SELECT (DATE '2020-01-01'::timestamp_ltz(9))::string",
      "SELECT (DATE '2020-01-01'::timestamp_ltz(7))::string",
      "SELECT TIMESTAMP_LTZ '2020-01-01 12:30:15.123456789'::date",
      "SELECT TIMESTAMP_LTZ '1960-01-01 00:00:00.000000001'::date",
      "SELECT DATE '2020-01-01'::timestamp_ltz(9)::date",
      "SELECT ((NULL::date)::timestamp_ltz(9))::string",
      "SELECT (NULL::timestamp_ltz(9))::date"
    ).foreach(checkDualRunMatchesFixedPoint)
  }
}
