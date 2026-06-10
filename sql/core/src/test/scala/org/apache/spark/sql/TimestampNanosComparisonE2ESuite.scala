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

import java.time.LocalDateTime

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end SQL coverage for comparison expressions over nanosecond timestamp types.
 *
 * Same-type comparisons (literals minted by the parser with 7-9 fractional digits) work; the
 * mixed-type tests at the bottom pin the current behavior of comparisons that need type
 * coercion, which is not wired up for the nanos types yet.
 */
class TimestampNanosComparisonE2ESuite extends QueryTest with SharedSparkSession {

  // NOTE: spark.sql.timestampNanosTypes.enabled defaults to true under Utils.isTesting, so no
  // conf override is needed here.

  private def ntz(frac: String): String = s"TIMESTAMP_NTZ'2026-01-01 00:00:00.$frac'"
  private def ltz(frac: String): String = s"TIMESTAMP'2026-01-01 00:00:00.$frac'"
  private def ldt(frac: String): LocalDateTime = LocalDateTime.parse(s"2026-01-01T00:00:00.$frac")

  test("NTZ nanos literal comparisons: sub-microsecond digits decide") {
    // 123456789 vs 123456790 differ only below the microsecond.
    checkAnswer(sql(s"SELECT ${ntz("123456789")} < ${ntz("123456790")}"), Row(true))
    checkAnswer(sql(s"SELECT ${ntz("123456789")} > ${ntz("123456790")}"), Row(false))
    checkAnswer(sql(s"SELECT ${ntz("123456789")} = ${ntz("123456789")}"), Row(true))
    checkAnswer(sql(s"SELECT ${ntz("123456789")} = ${ntz("123456790")}"), Row(false))
    checkAnswer(sql(s"SELECT ${ntz("123456789")} <=> ${ntz("123456789")}"), Row(true))
    checkAnswer(sql(s"SELECT ${ntz("123456789")} <= ${ntz("123456789")}"), Row(true))
    checkAnswer(sql(s"SELECT ${ntz("123456789")} >= ${ntz("123456790")}"), Row(false))
  }

  test("LTZ nanos literal comparisons") {
    checkAnswer(sql(s"SELECT ${ltz("123456789")} < ${ltz("123456790")}"), Row(true))
    checkAnswer(sql(s"SELECT ${ltz("123456789")} = ${ltz("123456789")}"), Row(true))
    checkAnswer(sql(s"SELECT ${ltz("123456790")} > ${ltz("123456789")}"), Row(true))
  }

  test("IN over same-type nanos literals") {
    checkAnswer(
      sql(s"SELECT ${ntz("123456789")} IN (${ntz("123456790")}, ${ntz("123456789")})"),
      Row(true))
    checkAnswer(
      sql(s"SELECT ${ntz("123456789")} IN (${ntz("123456790")}, ${ntz("123456791")})"),
      Row(false))
  }

  private val nanosValuesClause =
    s"VALUES (${ntz("000001002")}), (${ntz("000000999")}), (${ntz("000001001")}) AS t(ts)"

  test("ORDER BY sorts by sub-microsecond digits") {
    checkAnswer(
      sql(s"SELECT ts FROM $nanosValuesClause ORDER BY ts"),
      Seq(Row(ldt("000000999")), Row(ldt("000001001")), Row(ldt("000001002"))))
    checkAnswer(
      sql(s"SELECT ts FROM $nanosValuesClause ORDER BY ts DESC"),
      Seq(Row(ldt("000001002")), Row(ldt("000001001")), Row(ldt("000000999"))))
  }

  test("MIN and MAX over a nanos column") {
    checkAnswer(
      sql(s"SELECT min(ts), max(ts) FROM $nanosValuesClause"),
      Row(ldt("000000999"), ldt("000001002")))
  }

  test("WHERE filter with a nanos comparison") {
    checkAnswer(
      sql(s"SELECT count(*) FROM $nanosValuesClause WHERE ts > ${ntz("000001000")}"),
      Row(2L))
    checkAnswer(
      sql(s"SELECT count(*) FROM $nanosValuesClause WHERE ts <= ${ntz("000000999")}"),
      Row(1L))
  }

  private val joinQuery =
    s"""
       |SELECT a.id, b.id FROM
       |  (SELECT * FROM VALUES (1, ${ntz("123456789")}), (2, ${ntz("123456788")}) AS a(id, ts)) a
       |JOIN
       |  (SELECT * FROM VALUES (10, ${ntz("123456789")}) AS b(id, ts)) b
       |ON a.ts = b.ts
       |""".stripMargin

  test("join on nanos equality: values differing only in nanosWithinMicro must not match") {
    checkAnswer(sql(joinQuery), Row(1, 10))
  }

  test("sort-merge join on nanos keys") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      checkAnswer(sql(joinQuery), Row(1, 10))
    }
  }

  test("string vs nanos comparison works: the string side is coerced to the nanos type") {
    checkAnswer(
      sql(s"SELECT ${ntz("123456789")} = '2026-01-01 00:00:00.123456789'"), Row(true))
    // Sub-microsecond digits in the string participate in the comparison.
    checkAnswer(
      sql(s"SELECT ${ntz("123456789")} < '2026-01-01 00:00:00.123456790'"), Row(true))
    checkAnswer(
      sql(s"SELECT ${ntz("123456789")} = '2026-01-01 00:00:00.123456788'"), Row(false))
  }

  // ---------------------------------------------------------------------------------------------
  // Mixed-type comparisons: type coercion is NOT wired up for the nanos types yet.
  // `TypeCoercionHelper.findWiderDateTimeType` has no nanos arms, so any datetime-vs-datetime
  // pair involving a nanos type dies with a raw MatchError that surfaces to the user as
  // SparkException[INTERNAL_ERROR] instead of a proper analysis error. These tests pin the
  // current behavior and should be rewritten once nanos coercion lands.
  // ---------------------------------------------------------------------------------------------

  private def assertCoercionUnwired(name: String, query: String): Unit =
    test(s"mixed-type comparison currently fails as INTERNAL_ERROR: $name") {
      val e = intercept[SparkException](sql(query).collect())
      assert(e.getCondition == "INTERNAL_ERROR", s"got condition ${e.getCondition}")
      assert(e.getCause.isInstanceOf[MatchError],
        s"expected MatchError from non-exhaustive findWiderDateTimeType, got: ${e.getCause}")
    }

  assertCoercionUnwired("nanos(9) vs nanos(7)", s"SELECT ${ntz("123456789")} < ${ntz("1234567")}")
  assertCoercionUnwired("nanos(9) vs micros(6)", s"SELECT ${ntz("123456789")} < ${ntz("123456")}")
  assertCoercionUnwired(
    "nanos NTZ(9) vs nanos LTZ(9)", s"SELECT ${ntz("123456789")} < ${ltz("123456789")}")
  assertCoercionUnwired("nanos(9) vs DATE", s"SELECT DATE'2026-01-01' < ${ntz("123456789")}")
  assertCoercionUnwired(
    "IN over mixed nanos precisions",
    s"SELECT ${ntz("123456789")} IN (${ntz("1234567")}, ${ntz("123456789")})")
}
