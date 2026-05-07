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
import org.apache.spark.sql.catalyst.plans.logical.AddPartitions
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableAddPartitionParserSuite extends AnalysisTest with SharedSparkSession {
  test("add partition if not exists") {
    val sql = """
      |ALTER TABLE a.b.c ADD IF NOT EXISTS PARTITION
      |(dt='2008-08-08', country='us') LOCATION 'location1' PARTITION
      |(dt='2009-09-09', country='uk')""".stripMargin
    val parsed = parsePlan(sql)
    val expected = AddPartitions(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... ADD PARTITION ..."),
      Seq(
        UnresolvedPartitionSpec(Map("dt" -> "2008-08-08", "country" -> "us"), Some("location1")),
        UnresolvedPartitionSpec(Map("dt" -> "2009-09-09", "country" -> "uk"), None)),
      ifNotExists = true)
    comparePlans(parsed, expected)
  }

  test("add partition") {
    val sql = "ALTER TABLE a.b.c ADD PARTITION (dt='2008-08-08') LOCATION 'loc'"
    val parsed = parsePlan(sql)
    val expected = AddPartitions(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... ADD PARTITION ..."),
      Seq(UnresolvedPartitionSpec(Map("dt" -> "2008-08-08"), Some("loc"))),
      ifNotExists = false)

    comparePlans(parsed, expected)
  }
}
