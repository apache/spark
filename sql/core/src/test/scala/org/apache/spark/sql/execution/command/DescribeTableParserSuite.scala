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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute,
  UnresolvedPartitionSpec, UnresolvedTableOrView, UnresolvedTableOrViewSearchPathMode}
import org.apache.spark.sql.catalyst.plans.logical.{DescribeColumn, DescribeRelation,
  DescribeTablePartition}
import org.apache.spark.sql.test.SharedSparkSession

class DescribeTableParserSuite extends SharedSparkSession with AnalysisTest {
  private def parsePlan(statement: String) = spark.sessionState.sqlParser.parsePlan(statement)
  private def unresolvedDescribeTable(name: String): UnresolvedTableOrView = {
    UnresolvedTableOrView(
      Seq(name),
      "DESCRIBE TABLE",
      allowTempView = true,
      UnresolvedTableOrViewSearchPathMode.QueryLike)
  }

  test("SPARK-17328: Fix NPE with EXPLAIN DESCRIBE TABLE") {
    comparePlans(parsePlan("describe t"),
      DescribeRelation(
        unresolvedDescribeTable("t"), isExtended = false))
    comparePlans(parsePlan("describe table t"),
      DescribeRelation(
        unresolvedDescribeTable("t"), isExtended = false))
    comparePlans(parsePlan("describe table extended t"),
      DescribeRelation(
        unresolvedDescribeTable("t"), isExtended = true))
    comparePlans(parsePlan("describe table formatted t"),
      DescribeRelation(
        unresolvedDescribeTable("t"), isExtended = true))
  }

  test("describe table with partition spec") {
    comparePlans(parsePlan("DESCRIBE TABLE t PARTITION (ds='2024-01-01')"),
      DescribeTablePartition(
        unresolvedDescribeTable("t"),
        UnresolvedPartitionSpec(Map("ds" -> "2024-01-01")),
        isExtended = false))
  }

  test("describe table column") {
    comparePlans(parsePlan("DESCRIBE t col"),
      DescribeColumn(
        unresolvedDescribeTable("t"),
        UnresolvedAttribute(Seq("col")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE t `abc.xyz`"),
      DescribeColumn(
        unresolvedDescribeTable("t"),
        UnresolvedAttribute(Seq("abc.xyz")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE t abc.xyz"),
      DescribeColumn(
        unresolvedDescribeTable("t"),
        UnresolvedAttribute(Seq("abc", "xyz")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE t `a.b`.`x.y`"),
      DescribeColumn(
        unresolvedDescribeTable("t"),
        UnresolvedAttribute(Seq("a.b", "x.y")),
        isExtended = false))

    comparePlans(parsePlan("DESCRIBE TABLE t col"),
      DescribeColumn(
        unresolvedDescribeTable("t"),
        UnresolvedAttribute(Seq("col")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE TABLE EXTENDED t col"),
      DescribeColumn(
        unresolvedDescribeTable("t"),
        UnresolvedAttribute(Seq("col")),
        isExtended = true))
    comparePlans(parsePlan("DESCRIBE TABLE FORMATTED t col"),
      DescribeColumn(
        unresolvedDescribeTable("t"),
        UnresolvedAttribute(Seq("col")),
        isExtended = true))

    val error = intercept[AnalysisException](parsePlan("DESCRIBE EXTENDED t col AS JSON"))

    checkError(
      exception = error,
      condition = "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_JSON")

    val sql = "DESCRIBE TABLE t PARTITION (ds='1970-01-01') col"
    checkError(
      exception = parseException(parsePlan)(sql),
      condition = "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 47))
  }

  test("retain sql text position") {
    val tbl = "unknown"
    val sqlStatement = s"DESCRIBE TABLE $tbl"
    val startPos = sqlStatement.indexOf(tbl)
    assert(startPos != -1)
    assertAnalysisErrorCondition(
      parsePlan(sqlStatement),
      "TABLE_OR_VIEW_NOT_FOUND",
      Map("relationName" -> s"`$tbl`"),
      Array(ExpectedContext(tbl, startPos, startPos + tbl.length - 1))
    )
  }
}
