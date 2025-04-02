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

import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/** These tests exercise passing constant but non-literal OPTIONS lists, and folding them. */
class TableOptionsConstantFoldingSuite extends QueryTest with SharedSparkSession {
  val prefix = "create table t (col int) using json options "

  /** Helper method to create a table with a OPTIONS list and then check the resulting value. */
  def checkOption(createOption: String, expectedValue: String): Unit = {
    withTable("t") {
      sql(s"$prefix ('k' = $createOption)")
      sql("insert into t values (42)")
      checkAnswer(spark.table("t"), Seq(Row(42)))
      val actual = spark.table("t")
        .queryExecution.sparkPlan.asInstanceOf[FileSourceScanExec].relation.options
      assert(actual.get("k").get == expectedValue)
    }
  }

  test("SPARK-43529: Support constant expressions in CREATE/REPLACE TABLE OPTIONS") {
    checkOption("1 + 2", "3")
    checkOption("'a' || 'b'", "ab")
    checkOption("true or false", "true")
    checkOption("null", null)
    checkOption("cast('11 23:4:0' as interval day to second)",
      "INTERVAL '11 23:04:00' DAY TO SECOND")
    withSQLConf(SQLConf.LEGACY_EVAL_CURRENT_TIME.key -> "true") {
      checkOption("date_diff(current_date(), current_date())", "0")
    }
    checkOption("date_sub(date'2022-02-02', 1)", "2022-02-01")
    checkOption("timestampadd(microsecond, 5, timestamp'2022-02-28 00:00:00')",
      "2022-02-28 00:00:00.000005")
    checkOption("round(cast(2.25 as decimal(5, 3)), 1)", "2.3")
    // The result of invoking this "ROUND" function call is NULL, since the target decimal type is
    // too narrow to contain the result of the cast.
    val cannotBeRepresented = "round(cast(2.25 as decimal(3, 3)), 1)"
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        if (ansiEnabled) {
          val exception = intercept[AnalysisException](sql(s"$prefix ('k' = $cannotBeRepresented)"))
          assert(exception.cause.exists(_.getMessage.contains(
            "2.25 cannot be represented as Decimal(3, 3)")))
        } else {
          checkOption(cannotBeRepresented, "null")
        }
      }
    }

    // Test some cases where the provided option value is a non-constant or invalid expression.
    checkError(
      exception = intercept[AnalysisException](
        sql(s"$prefix ('k' = 1 + 2 + unresolvedAttribute)")),
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      parameters = Map(
        "objectName" -> "`unresolvedAttribute`"),
      queryContext = Array(ExpectedContext("", "", 60, 78, "unresolvedAttribute")))
    checkError(
      exception = intercept[AnalysisException](
        sql(s"$prefix ('k' = true or false or unresolvedAttribute)")),
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      parameters = Map(
        "objectName" -> "`unresolvedAttribute`"),
      queryContext = Array(ExpectedContext("", "", 69, 87, "unresolvedAttribute")))
    checkError(
      exception = intercept[AnalysisException](
        sql(s"$prefix ('k' = cast(array('9', '9') as array<byte>))")),
      condition = "INVALID_SQL_SYNTAX.OPTION_IS_INVALID",
      parameters = Map(
        "key" -> "k",
        "supported" -> "constant expressions"))
    checkError(
      exception = intercept[AnalysisException](
        sql(s"$prefix ('k' = cast(map('9', '9') as map<string, string>))")),
      condition = "INVALID_SQL_SYNTAX.OPTION_IS_INVALID",
      parameters = Map(
        "key" -> "k",
        "supported" -> "constant expressions"))
    checkError(
      exception = intercept[AnalysisException](
        sql(s"$prefix ('k' = raise_error('failure'))")),
      condition = "INVALID_SQL_SYNTAX.OPTION_IS_INVALID",
      parameters = Map(
        "key" -> "k",
        "supported" -> "constant expressions"))
    checkError(
      exception = intercept[AnalysisException](
        sql(s"$prefix ('k' = raise_error('failure'))")),
      condition = "INVALID_SQL_SYNTAX.OPTION_IS_INVALID",
      parameters = Map(
        "key" -> "k",
        "supported" -> "constant expressions"))
  }
}
