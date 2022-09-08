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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedPartitionSpec, UnresolvedTable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{TruncatePartition, TruncateTable}
import org.apache.spark.sql.errors.QueryErrorsSuiteBase

class TruncateTableParserSuite extends AnalysisTest with QueryErrorsSuiteBase {
  test("truncate table") {
    comparePlans(
      parsePlan("TRUNCATE TABLE a.b.c"),
      TruncateTable(UnresolvedTable(Seq("a", "b", "c"), "TRUNCATE TABLE", None)))
  }

  test("truncate a single part partition") {
    comparePlans(
      parsePlan("TRUNCATE TABLE a.b.c PARTITION(ds='2017-06-10')"),
      TruncatePartition(
        UnresolvedTable(Seq("a", "b", "c"), "TRUNCATE TABLE", None),
        UnresolvedPartitionSpec(Map("ds" -> "2017-06-10"), None)))
  }

  test("truncate a multi parts partition") {
    comparePlans(
      parsePlan("TRUNCATE TABLE ns.tbl PARTITION(a = 1, B = 'ABC')"),
      TruncatePartition(
        UnresolvedTable(Seq("ns", "tbl"), "TRUNCATE TABLE", None),
        UnresolvedPartitionSpec(Map("a" -> "1", "B" -> "ABC"), None)))
  }

  test("empty values in non-optional partition specs") {
    checkError(
      exception = intercept[ParseException] {
        parsePlan("TRUNCATE TABLE dbx.tab1 PARTITION (a='1', b)")
      },
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Partition key `b` must set value (can't be empty)."),
      context = ExpectedContext(
        fragment = "PARTITION (a='1', b)",
        start = 24,
        stop = 43))
  }
}
