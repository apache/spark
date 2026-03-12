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
import org.apache.spark.sql.catalyst.plans.logical.RenamePartitions
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableRenamePartitionParserSuite extends AnalysisTest with SharedSparkSession {
  test("rename a partition with single part") {
    val sql = """
      |ALTER TABLE a.b.c PARTITION (ds='2017-06-10')
      |RENAME TO PARTITION (ds='2018-06-10')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = RenamePartitions(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... RENAME TO PARTITION"),
      UnresolvedPartitionSpec(Map("ds" -> "2017-06-10")),
      UnresolvedPartitionSpec(Map("ds" -> "2018-06-10")))
    comparePlans(parsed, expected)
  }

  test("rename a partition with multi parts") {
    val sql = """
      |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
      |RENAME TO PARTITION (dt='2008-09-09', country='uk')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = RenamePartitions(
      UnresolvedTable(
        Seq("table_name"),
        "ALTER TABLE ... RENAME TO PARTITION"),
      UnresolvedPartitionSpec(Map("dt" -> "2008-08-08", "country" -> "us")),
      UnresolvedPartitionSpec(Map("dt" -> "2008-09-09", "country" -> "uk")))
    comparePlans(parsed, expected)
  }
}
