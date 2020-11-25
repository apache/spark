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

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedPartitionSpec, UnresolvedTable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableAddPartition, AlterTableDropPartition, AlterTableRecoverPartitionsStatement, AlterTableRenamePartitionStatement}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSparkSession

class AlterTablePartitionParserSuite extends AnalysisTest with SharedSparkSession {
  private lazy val parser = new SparkSqlParser()

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parser.parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
    containsThesePhrases.foreach { p =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
    }
  }

  test("ALTER TABLE .. ADD PARTITION") {
    val sql1 =
      """
        |ALTER TABLE a.b.c ADD IF NOT EXISTS PARTITION
        |(dt='2008-08-08', country='us') LOCATION 'location1' PARTITION
        |(dt='2009-09-09', country='uk')
      """.stripMargin
    val sql2 = "ALTER TABLE a.b.c ADD PARTITION (dt='2008-08-08') LOCATION 'loc'"

    val parsed1 = parsePlan(sql1)
    val parsed2 = parsePlan(sql2)

    val expected1 = AlterTableAddPartition(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... ADD PARTITION ..."),
      Seq(
        UnresolvedPartitionSpec(Map("dt" -> "2008-08-08", "country" -> "us"), Some("location1")),
        UnresolvedPartitionSpec(Map("dt" -> "2009-09-09", "country" -> "uk"), None)),
      ifNotExists = true)
    val expected2 = AlterTableAddPartition(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... ADD PARTITION ..."),
      Seq(UnresolvedPartitionSpec(Map("dt" -> "2008-08-08"), Some("loc"))),
      ifNotExists = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("ALTER TABLE .. DROP PARTITION") {
    val sql1_table =
      """
        |ALTER TABLE table_name DROP IF EXISTS PARTITION
        |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val sql2_table =
      """
        |ALTER TABLE table_name DROP PARTITION
        |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val sql1_view = sql1_table.replace("TABLE", "VIEW")
    val sql2_view = sql2_table.replace("TABLE", "VIEW")

    val parsed1_table = parsePlan(sql1_table)
    val parsed2_table = parsePlan(sql2_table)
    val parsed1_purge = parsePlan(sql1_table + " PURGE")

    assertUnsupported(sql1_view)
    assertUnsupported(sql2_view)

    val expected1_table = AlterTableDropPartition(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... DROP PARTITION ..."),
      Seq(
        UnresolvedPartitionSpec(Map("dt" -> "2008-08-08", "country" -> "us")),
        UnresolvedPartitionSpec(Map("dt" -> "2009-09-09", "country" -> "uk"))),
      ifExists = true,
      purge = false,
      retainData = false)
    val expected2_table = expected1_table.copy(ifExists = false)
    val expected1_purge = expected1_table.copy(purge = true)

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
    comparePlans(parsed1_purge, expected1_purge)

    val sql3_table = "ALTER TABLE a.b.c DROP IF EXISTS PARTITION (ds='2017-06-10')"
    val expected3_table = AlterTableDropPartition(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... DROP PARTITION ..."),
      Seq(UnresolvedPartitionSpec(Map("ds" -> "2017-06-10"))),
      ifExists = true,
      purge = false,
      retainData = false)

    val parsed3_table = parsePlan(sql3_table)
    comparePlans(parsed3_table, expected3_table)
  }

  test("ALTER TABLE .. RECOVER PARTITIONS") {
    comparePlans(
      parsePlan("ALTER TABLE a.b.c RECOVER PARTITIONS"),
      AlterTableRecoverPartitionsStatement(Seq("a", "b", "c")))
  }

  test("ALTER TABLE .. PARTITION RENAME") {
    val sql1 =
      """
        |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
        |RENAME TO PARTITION (dt='2008-09-09', country='uk')
      """.stripMargin
    val parsed1 = parsePlan(sql1)
    val expected1 = AlterTableRenamePartitionStatement(
      Seq("table_name"),
      Map("dt" -> "2008-08-08", "country" -> "us"),
      Map("dt" -> "2008-09-09", "country" -> "uk"))
    comparePlans(parsed1, expected1)

    val sql2 =
      """
        |ALTER TABLE a.b.c PARTITION (ds='2017-06-10')
        |RENAME TO PARTITION (ds='2018-06-10')
      """.stripMargin
    val parsed2 = parsePlan(sql2)
    val expected2 = AlterTableRenamePartitionStatement(
      Seq("a", "b", "c"),
      Map("ds" -> "2017-06-10"),
      Map("ds" -> "2018-06-10"))
    comparePlans(parsed2, expected2)
  }

  test("alter table: exchange partition (not supported)") {
    assertUnsupported(
      """
        |ALTER TABLE table_name_1 EXCHANGE PARTITION
        |(dt='2008-08-08', country='us') WITH TABLE table_name_2
      """.stripMargin)
  }

  test("alter table: archive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name ARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: unarchive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name UNARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: set file format (not allowed)") {
    assertUnsupported(
      "ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test' OUTPUTFORMAT 'test'")
    assertUnsupported(
      "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
        "SET FILEFORMAT PARQUET")
  }

  test("alter table: touch (not supported)") {
    assertUnsupported("ALTER TABLE table_name TOUCH")
    assertUnsupported("ALTER TABLE table_name TOUCH PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: compact (not supported)") {
    assertUnsupported("ALTER TABLE table_name COMPACT 'compaction_type'")
    assertUnsupported(
      """
        |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
        |COMPACT 'MAJOR'
      """.stripMargin)
  }

  test("alter table: concatenate (not supported)") {
    assertUnsupported("ALTER TABLE table_name CONCATENATE")
    assertUnsupported(
      "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') CONCATENATE")
  }

  test("duplicate columns in partition specs") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        "ALTER TABLE dbx.tab1 PARTITION (a='1', a='2') RENAME TO PARTITION (a='100', a='200')")
    }.getMessage
    assert(e.contains("Found duplicate keys 'a'"))
  }
}
