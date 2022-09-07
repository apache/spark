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
import org.apache.spark.sql.catalyst.plans.logical.ShowPartitions
import org.apache.spark.sql.errors.QueryErrorsSuiteBase

class ShowPartitionsParserSuite extends AnalysisTest with QueryErrorsSuiteBase {
  test("SHOW PARTITIONS") {
    val commandName = "SHOW PARTITIONS"
    Seq(
      "SHOW PARTITIONS t1" -> ShowPartitions(UnresolvedTable(Seq("t1"), commandName, None), None),
      "SHOW PARTITIONS db1.t1" -> ShowPartitions(
        UnresolvedTable(Seq("db1", "t1"), commandName, None), None),
      "SHOW PARTITIONS t1 PARTITION(partcol1='partvalue', partcol2='partvalue')" ->
        ShowPartitions(
          UnresolvedTable(Seq("t1"), commandName, None),
          Some(UnresolvedPartitionSpec(Map("partcol1" -> "partvalue", "partcol2" -> "partvalue")))),
      "SHOW PARTITIONS a.b.c" -> ShowPartitions(
        UnresolvedTable(Seq("a", "b", "c"), commandName, None), None),
      "SHOW PARTITIONS a.b.c PARTITION(ds='2017-06-10')" ->
        ShowPartitions(
          UnresolvedTable(Seq("a", "b", "c"), commandName, None),
          Some(UnresolvedPartitionSpec(Map("ds" -> "2017-06-10"))))
    ).foreach { case (sql, expected) =>
      val parsed = parsePlan(sql)
      comparePlans(parsed, expected)
    }
  }

  test("empty values in non-optional partition specs") {
    checkError(
      exception = intercept[ParseException] {
        parsePlan("SHOW PARTITIONS dbx.tab1 PARTITION (a='1', b)")
      },
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Partition key `b` must set value (can't be empty)."),
      context = ExpectedContext(
        fragment = "PARTITION (a='1', b)",
        start = 25,
        stop = 44))
  }
}
