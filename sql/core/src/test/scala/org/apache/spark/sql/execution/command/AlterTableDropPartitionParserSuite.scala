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
import org.apache.spark.sql.catalyst.plans.logical.DropPartitions
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableDropPartitionParserSuite extends AnalysisTest with SharedSparkSession {

  test("drop partition") {
    val sql = """
      |ALTER TABLE table_name DROP PARTITION
      |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val expected = DropPartitions(
      UnresolvedTable(
        Seq("table_name"),
        "ALTER TABLE ... DROP PARTITION ..."),
      Seq(
        UnresolvedPartitionSpec(Map("dt" -> "2008-08-08", "country" -> "us")),
        UnresolvedPartitionSpec(Map("dt" -> "2009-09-09", "country" -> "uk"))),
      ifExists = false,
      purge = false)

    comparePlans(parsePlan(sql), expected)
  }

  test("drop partition if exists") {
    val sql = """
      |ALTER TABLE table_name DROP IF EXISTS
      |PARTITION (dt='2008-08-08', country='us'),
      |PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val expected = DropPartitions(
      UnresolvedTable(
        Seq("table_name"),
        "ALTER TABLE ... DROP PARTITION ..."),
      Seq(
        UnresolvedPartitionSpec(Map("dt" -> "2008-08-08", "country" -> "us")),
        UnresolvedPartitionSpec(Map("dt" -> "2009-09-09", "country" -> "uk"))),
      ifExists = true,
      purge = false)
    comparePlans(parsePlan(sql), expected)
  }

  test("drop partition in a table with multi-part identifier") {
    val sql = "ALTER TABLE a.b.c DROP IF EXISTS PARTITION (ds='2017-06-10')"
    val expected = DropPartitions(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... DROP PARTITION ..."),
      Seq(UnresolvedPartitionSpec(Map("ds" -> "2017-06-10"))),
      ifExists = true,
      purge = false)

    comparePlans(parsePlan(sql), expected)
  }

  test("drop partition with PURGE") {
    val sql = "ALTER TABLE table_name DROP PARTITION (p=1) PURGE"
    val expected = DropPartitions(
      UnresolvedTable(
        Seq("table_name"),
        "ALTER TABLE ... DROP PARTITION ..."),
      Seq(UnresolvedPartitionSpec(Map("p" -> "1"))),
      ifExists = false,
      purge = true)

    comparePlans(parsePlan(sql), expected)
  }

  test("drop partition from view") {
    val sql = "ALTER VIEW table_name DROP PARTITION (p=1)"
    checkError(
      exception = parseException(parsePlan)(sql),
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER VIEW ... DROP PARTITION"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 41))
  }
}
