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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.types._

class DDLCommandSuite extends PlanTest {
  private val parser = SparkSqlParser

  test("create database") {
    val sql =
      """
       |CREATE DATABASE IF NOT EXISTS database_name
       |COMMENT 'database_comment' LOCATION '/home/user/db'
       |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = CreateDatabase(
      "database_name",
      ifNotExists = true,
      Some("/home/user/db"),
      Some("database_comment"),
      Map("a" -> "a", "b" -> "b", "c" -> "c"))
    comparePlans(parsed, expected)
  }

  test("drop database") {
    val sql1 = "DROP DATABASE IF EXISTS database_name RESTRICT"
    val sql2 = "DROP DATABASE IF EXISTS database_name CASCADE"
    val sql3 = "DROP SCHEMA IF EXISTS database_name RESTRICT"
    val sql4 = "DROP SCHEMA IF EXISTS database_name CASCADE"
    // The default is restrict=true
    val sql5 = "DROP DATABASE IF EXISTS database_name"
    // The default is ifExists=false
    val sql6 = "DROP DATABASE database_name"
    val sql7 = "DROP DATABASE database_name CASCADE"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)
    val parsed5 = parser.parsePlan(sql5)
    val parsed6 = parser.parsePlan(sql6)
    val parsed7 = parser.parsePlan(sql7)

    val expected1 = DropDatabase(
      "database_name",
      ifExists = true,
      cascade = false)
    val expected2 = DropDatabase(
      "database_name",
      ifExists = true,
      cascade = true)
    val expected3 = DropDatabase(
      "database_name",
      ifExists = false,
      cascade = false)
    val expected4 = DropDatabase(
      "database_name",
      ifExists = false,
      cascade = true)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected1)
    comparePlans(parsed4, expected2)
    comparePlans(parsed5, expected1)
    comparePlans(parsed6, expected3)
    comparePlans(parsed7, expected4)
  }

  test("alter database set dbproperties") {
    // ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...)
    val sql1 = "ALTER DATABASE database_name SET DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')"
    val sql2 = "ALTER SCHEMA database_name SET DBPROPERTIES ('a'='a')"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = AlterDatabaseProperties(
      "database_name",
      Map("a" -> "a", "b" -> "b", "c" -> "c"))
    val expected2 = AlterDatabaseProperties(
      "database_name",
      Map("a" -> "a"))

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("describe database") {
    // DESCRIBE DATABASE [EXTENDED] db_name;
    val sql1 = "DESCRIBE DATABASE EXTENDED db_name"
    val sql2 = "DESCRIBE DATABASE db_name"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = DescribeDatabase(
      "db_name",
      extended = true)
    val expected2 = DescribeDatabase(
      "db_name",
      extended = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("create function") {
    val sql1 =
      """
       |CREATE TEMPORARY FUNCTION helloworld as
       |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
       |JAR '/path/to/jar2'
     """.stripMargin
    val sql2 =
      """
        |CREATE FUNCTION hello.world as
        |'com.matthewrathbone.example.SimpleUDFExample' USING ARCHIVE '/path/to/archive',
        |FILE '/path/to/file'
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val expected1 = CreateFunction(
      None,
      "helloworld",
      "com.matthewrathbone.example.SimpleUDFExample",
      Seq(("jar", "/path/to/jar1"), ("jar", "/path/to/jar2")),
      isTemp = true)
    val expected2 = CreateFunction(
      Some("hello"),
      "world",
      "com.matthewrathbone.example.SimpleUDFExample",
      Seq(("archive", "/path/to/archive"), ("file", "/path/to/file")),
      isTemp = false)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("drop function") {
    val sql1 = "DROP TEMPORARY FUNCTION helloworld"
    val sql2 = "DROP TEMPORARY FUNCTION IF EXISTS helloworld"
    val sql3 = "DROP FUNCTION hello.world"
    val sql4 = "DROP FUNCTION IF EXISTS hello.world"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)

    val expected1 = DropFunction(
      None,
      "helloworld",
      ifExists = false,
      isTemp = true)
    val expected2 = DropFunction(
      None,
      "helloworld",
      ifExists = true,
      isTemp = true)
    val expected3 = DropFunction(
      Some("hello"),
      "world",
      ifExists = false,
      isTemp = false)
    val expected4 = DropFunction(
      Some("hello"),
      "world",
      ifExists = true,
      isTemp = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  // ALTER TABLE table_name RENAME TO new_table_name;
  // ALTER VIEW view_name RENAME TO new_view_name;
  test("alter table/view: rename table/view") {
    val sql_table = "ALTER TABLE table_name RENAME TO new_table_name"
    val sql_view = sql_table.replace("TABLE", "VIEW")
    val parsed_table = parser.parsePlan(sql_table)
    val parsed_view = parser.parsePlan(sql_view)
    val expected_table = AlterTableRename(
      TableIdentifier("table_name", None),
      TableIdentifier("new_table_name", None))(sql_table)
    val expected_view = AlterTableRename(
      TableIdentifier("table_name", None),
      TableIdentifier("new_table_name", None))(sql_view)
    comparePlans(parsed_table, expected_table)
    comparePlans(parsed_view, expected_view)
  }

  // ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER TABLE table_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  // ALTER VIEW view_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER VIEW view_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter table/view: alter table/view properties") {
    val sql1_table = "ALTER TABLE table_name SET TBLPROPERTIES ('test' = 'test', " +
      "'comment' = 'new_comment')"
    val sql2_table = "ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_table = "ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"
    val sql1_view = sql1_table.replace("TABLE", "VIEW")
    val sql2_view = sql2_table.replace("TABLE", "VIEW")
    val sql3_view = sql3_table.replace("TABLE", "VIEW")

    val parsed1_table = parser.parsePlan(sql1_table)
    val parsed2_table = parser.parsePlan(sql2_table)
    val parsed3_table = parser.parsePlan(sql3_table)
    val parsed1_view = parser.parsePlan(sql1_view)
    val parsed2_view = parser.parsePlan(sql2_view)
    val parsed3_view = parser.parsePlan(sql3_view)

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_table = AlterTableSetProperties(
      tableIdent, Map("test" -> "test", "comment" -> "new_comment"))(sql1_table)
    val expected2_table = AlterTableUnsetProperties(
      tableIdent, Map("comment" -> null, "test" -> null), ifExists = false)(sql2_table)
    val expected3_table = AlterTableUnsetProperties(
      tableIdent, Map("comment" -> null, "test" -> null), ifExists = true)(sql3_table)
    val expected1_view = expected1_table.copy()(sql = sql1_view)
    val expected2_view = expected2_table.copy()(sql = sql2_view)
    val expected3_view = expected3_table.copy()(sql = sql3_view)

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
    comparePlans(parsed3_table, expected3_table)
    comparePlans(parsed1_view, expected1_view)
    comparePlans(parsed2_view, expected2_view)
    comparePlans(parsed3_view, expected3_view)
  }

  test("alter table: SerDe properties") {
    val sql1 = "ALTER TABLE table_name SET SERDE 'org.apache.class'"
    val sql2 =
      """
       |ALTER TABLE table_name SET SERDE 'org.apache.class'
       |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val sql3 =
      """
       |ALTER TABLE table_name SET SERDEPROPERTIES ('columns'='foo,bar',
       |'field.delim' = ',')
      """.stripMargin
    val sql4 =
      """
       |ALTER TABLE table_name PARTITION (test, dt='2008-08-08',
       |country='us') SET SERDE 'org.apache.class' WITH SERDEPROPERTIES ('columns'='foo,bar',
       |'field.delim' = ',')
      """.stripMargin
    val sql5 =
      """
       |ALTER TABLE table_name PARTITION (test, dt='2008-08-08',
       |country='us') SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)
    val parsed5 = parser.parsePlan(sql5)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSerDeProperties(
      tableIdent, Some("org.apache.class"), None, None)(sql1)
    val expected2 = AlterTableSerDeProperties(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)(sql2)
    val expected3 = AlterTableSerDeProperties(
      tableIdent, None, Some(Map("columns" -> "foo,bar", "field.delim" -> ",")), None)(sql3)
    val expected4 = AlterTableSerDeProperties(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> null, "dt" -> "2008-08-08", "country" -> "us")))(sql4)
    val expected5 = AlterTableSerDeProperties(
      tableIdent,
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> null, "dt" -> "2008-08-08", "country" -> "us")))(sql5)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
    comparePlans(parsed5, expected5)
  }

  test("alter table: storage properties") {
    val sql1 = "ALTER TABLE table_name CLUSTERED BY (dt, country) INTO 10 BUCKETS"
    val sql2 = "ALTER TABLE table_name CLUSTERED BY (dt, country) SORTED BY " +
      "(dt, country DESC) INTO 10 BUCKETS"
    val sql3 = "ALTER TABLE table_name NOT CLUSTERED"
    val sql4 = "ALTER TABLE table_name NOT SORTED"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)
    val tableIdent = TableIdentifier("table_name", None)
    val cols = List("dt", "country")
    // TODO: also test the sort directions once we keep track of that
    val expected1 = AlterTableStorageProperties(
      tableIdent, BucketSpec(10, cols, Nil))(sql1)
    val expected2 = AlterTableStorageProperties(
      tableIdent, BucketSpec(10, cols, cols))(sql2)
    val expected3 = AlterTableNotClustered(tableIdent)(sql3)
    val expected4 = AlterTableNotSorted(tableIdent)(sql4)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  test("alter table: skewed") {
    val sql1 =
      """
       |ALTER TABLE table_name SKEWED BY (dt, country) ON
       |(('2008-08-08', 'us'), ('2009-09-09', 'uk'), ('2010-10-10', 'cn')) STORED AS DIRECTORIES
      """.stripMargin
    val sql2 =
      """
       |ALTER TABLE table_name SKEWED BY (dt, country) ON
       |('2008-08-08', 'us') STORED AS DIRECTORIES
      """.stripMargin
    val sql3 =
      """
       |ALTER TABLE table_name SKEWED BY (dt, country) ON
       |(('2008-08-08', 'us'), ('2009-09-09', 'uk'))
      """.stripMargin
    val sql4 = "ALTER TABLE table_name NOT SKEWED"
    val sql5 = "ALTER TABLE table_name NOT STORED AS DIRECTORIES"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)
    val parsed5 = parser.parsePlan(sql5)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSkewed(
      tableIdent,
      Seq("dt", "country"),
      Seq(List("2008-08-08", "us"), List("2009-09-09", "uk"), List("2010-10-10", "cn")),
      storedAsDirs = true)(sql1)
    val expected2 = AlterTableSkewed(
      tableIdent,
      Seq("dt", "country"),
      Seq(List("2008-08-08", "us")),
      storedAsDirs = true)(sql2)
    val expected3 = AlterTableSkewed(
      tableIdent,
      Seq("dt", "country"),
      Seq(List("2008-08-08", "us"), List("2009-09-09", "uk")),
      storedAsDirs = false)(sql3)
    val expected4 = AlterTableNotSkewed(tableIdent)(sql4)
    val expected5 = AlterTableNotStoredAsDirs(tableIdent)(sql5)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
    comparePlans(parsed5, expected5)
  }

  test("alter table: skewed location") {
    val sql1 =
      """
       |ALTER TABLE table_name SET SKEWED LOCATION
       |('123'='location1', 'test'='location2')
      """.stripMargin
    val sql2 =
      """
       |ALTER TABLE table_name SET SKEWED LOCATION
       |(('2008-08-08', 'us')='location1', 'test'='location2')
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSkewedLocation(
      tableIdent,
      Map("123" -> "location1", "test" -> "location2"))(sql1)
    val expected2 = AlterTableSkewedLocation(
      tableIdent,
      Map("2008-08-08" -> "location1", "us" -> "location1", "test" -> "location2"))(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  // ALTER TABLE table_name ADD [IF NOT EXISTS] PARTITION partition_spec
  // [LOCATION 'location1'] partition_spec [LOCATION 'location2'] ...;
  test("alter table: add partition") {
    val sql1 =
      """
       |ALTER TABLE table_name ADD IF NOT EXISTS PARTITION
       |(dt='2008-08-08', country='us') LOCATION 'location1' PARTITION
       |(dt='2009-09-09', country='uk')
      """.stripMargin
    val sql2 = "ALTER TABLE table_name ADD PARTITION (dt='2008-08-08') LOCATION 'loc'"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = AlterTableAddPartition(
      TableIdentifier("table_name", None),
      Seq(
        (Map("dt" -> "2008-08-08", "country" -> "us"), Some("location1")),
        (Map("dt" -> "2009-09-09", "country" -> "uk"), None)),
      ifNotExists = true)(sql1)
    val expected2 = AlterTableAddPartition(
      TableIdentifier("table_name", None),
      Seq((Map("dt" -> "2008-08-08"), Some("loc"))),
      ifNotExists = false)(sql2)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  // ALTER VIEW view_name ADD [IF NOT EXISTS] PARTITION partition_spec PARTITION partition_spec ...;
  test("alter view: add partition") {
    val sql1 =
      """
        |ALTER VIEW view_name ADD IF NOT EXISTS PARTITION
        |(dt='2008-08-08', country='us') PARTITION
        |(dt='2009-09-09', country='uk')
      """.stripMargin
    // different constant types in partitioning spec
    val sql2 =
    """
      |ALTER VIEW view_name ADD PARTITION
      |(col1=NULL, cOL2='f', col3=5, COL4=true)
    """.stripMargin

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = AlterTableAddPartition(
      TableIdentifier("view_name", None),
      Seq(
        (Map("dt" -> "2008-08-08", "country" -> "us"), None),
        (Map("dt" -> "2009-09-09", "country" -> "uk"), None)),
      ifNotExists = true)(sql1)
    val expected2 = AlterTableAddPartition(
      TableIdentifier("view_name", None),
      Seq((Map("col1" -> "NULL", "col2" -> "f", "col3" -> "5", "col4" -> "true"), None)),
      ifNotExists = false)(sql2)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: rename partition") {
    val sql =
      """
       |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
       |RENAME TO PARTITION (dt='2008-09-09', country='uk')
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableRenamePartition(
      TableIdentifier("table_name", None),
      Map("dt" -> "2008-08-08", "country" -> "us"),
      Map("dt" -> "2008-09-09", "country" -> "uk"))(sql)
    comparePlans(parsed, expected)
  }

  test("alter table: exchange partition") {
    val sql =
      """
       |ALTER TABLE table_name_1 EXCHANGE PARTITION
       |(dt='2008-08-08', country='us') WITH TABLE table_name_2
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableExchangePartition(
      TableIdentifier("table_name_1", None),
      TableIdentifier("table_name_2", None),
      Map("dt" -> "2008-08-08", "country" -> "us"))(sql)
    comparePlans(parsed, expected)
  }

  // ALTER TABLE table_name DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE]
  // ALTER VIEW table_name DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...]
  test("alter table/view: drop partitions") {
    val sql1_table =
      """
       |ALTER TABLE table_name DROP IF EXISTS PARTITION
       |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val sql2_table =
      """
       |ALTER TABLE table_name DROP PARTITION
       |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk') PURGE
      """.stripMargin
    val sql1_view = sql1_table.replace("TABLE", "VIEW")
    // Note: ALTER VIEW DROP PARTITION does not support PURGE
    val sql2_view = sql2_table.replace("TABLE", "VIEW").replace("PURGE", "")

    val parsed1_table = parser.parsePlan(sql1_table)
    val parsed2_table = parser.parsePlan(sql2_table)
    val parsed1_view = parser.parsePlan(sql1_view)
    val parsed2_view = parser.parsePlan(sql2_view)

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_table = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = true,
      purge = false)(sql1_table)
    val expected2_table = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = false,
      purge = true)(sql2_table)

    val expected1_view = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = true,
      purge = false)(sql1_view)
    val expected2_view = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = false,
      purge = false)(sql2_table)

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
    comparePlans(parsed1_view, expected1_view)
    comparePlans(parsed2_view, expected2_view)
  }

  test("alter table: archive partition") {
    val sql = "ALTER TABLE table_name ARCHIVE PARTITION (dt='2008-08-08', country='us')"
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableArchivePartition(
      TableIdentifier("table_name", None),
      Map("dt" -> "2008-08-08", "country" -> "us"))(sql)
    comparePlans(parsed, expected)
  }

  test("alter table: unarchive partition") {
    val sql = "ALTER TABLE table_name UNARCHIVE PARTITION (dt='2008-08-08', country='us')"
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableUnarchivePartition(
      TableIdentifier("table_name", None),
      Map("dt" -> "2008-08-08", "country" -> "us"))(sql)
    comparePlans(parsed, expected)
  }

  test("alter table: set file format") {
    val sql1 =
      """
       |ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test'
       |OUTPUTFORMAT 'test' SERDE 'test' INPUTDRIVER 'test' OUTPUTDRIVER 'test'
      """.stripMargin
    val sql2 = "ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test' " +
      "OUTPUTFORMAT 'test' SERDE 'test'"
    val sql3 = "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "SET FILEFORMAT PARQUET"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSetFileFormat(
      tableIdent,
      None,
      List("test", "test", "test", "test", "test"),
      None)(sql1)
    val expected2 = AlterTableSetFileFormat(
      tableIdent,
      None,
      List("test", "test", "test"),
      None)(sql2)
    val expected3 = AlterTableSetFileFormat(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      Seq(),
      Some("PARQUET"))(sql3)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
  }

  test("alter table: set location") {
    val sql1 = "ALTER TABLE table_name SET LOCATION 'new location'"
    val sql2 = "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "SET LOCATION 'new location'"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSetLocation(
      tableIdent,
      None,
      "new location")(sql1)
    val expected2 = AlterTableSetLocation(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      "new location")(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: touch") {
    val sql1 = "ALTER TABLE table_name TOUCH"
    val sql2 = "ALTER TABLE table_name TOUCH PARTITION (dt='2008-08-08', country='us')"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableTouch(
      tableIdent,
      None)(sql1)
    val expected2 = AlterTableTouch(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")))(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: compact") {
    val sql1 = "ALTER TABLE table_name COMPACT 'compaction_type'"
    val sql2 =
      """
       |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
       |COMPACT 'MAJOR'
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableCompact(
      tableIdent,
      None,
      "compaction_type")(sql1)
    val expected2 = AlterTableCompact(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      "MAJOR")(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: concatenate") {
    val sql1 = "ALTER TABLE table_name CONCATENATE"
    val sql2 = "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') CONCATENATE"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableMerge(tableIdent, None)(sql1)
    val expected2 = AlterTableMerge(
      tableIdent, Some(Map("dt" -> "2008-08-08", "country" -> "us")))(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: change column name/type/position/comment") {
    val sql1 = "ALTER TABLE table_name CHANGE col_old_name col_new_name INT"
    val sql2 =
      """
       |ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT
       |COMMENT 'col_comment' FIRST CASCADE
      """.stripMargin
    val sql3 =
      """
       |ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT
       |COMMENT 'col_comment' AFTER column_name RESTRICT
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableChangeCol(
      tableName = tableIdent,
      partitionSpec = None,
      oldColName = "col_old_name",
      newColName = "col_new_name",
      dataType = IntegerType,
      comment = None,
      afterColName = None,
      restrict = false,
      cascade = false)(sql1)
    val expected2 = AlterTableChangeCol(
      tableName = tableIdent,
      partitionSpec = None,
      oldColName = "col_old_name",
      newColName = "col_new_name",
      dataType = IntegerType,
      comment = Some("col_comment"),
      afterColName = None,
      restrict = false,
      cascade = true)(sql2)
    val expected3 = AlterTableChangeCol(
      tableName = tableIdent,
      partitionSpec = None,
      oldColName = "col_old_name",
      newColName = "col_new_name",
      dataType = IntegerType,
      comment = Some("col_comment"),
      afterColName = Some("column_name"),
      restrict = true,
      cascade = false)(sql3)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
  }

  test("alter table: add/replace columns") {
    val sql1 =
      """
       |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
       |ADD COLUMNS (new_col1 INT COMMENT 'test_comment', new_col2 LONG
       |COMMENT 'test_comment2') CASCADE
      """.stripMargin
    val sql2 =
      """
       |ALTER TABLE table_name REPLACE COLUMNS (new_col1 INT
       |COMMENT 'test_comment', new_col2 LONG COMMENT 'test_comment2') RESTRICT
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val meta1 = new MetadataBuilder().putString("comment", "test_comment").build()
    val meta2 = new MetadataBuilder().putString("comment", "test_comment2").build()
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableAddCol(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      StructType(Seq(
        StructField("new_col1", IntegerType, nullable = true, meta1),
        StructField("new_col2", LongType, nullable = true, meta2))),
      restrict = false,
      cascade = true)(sql1)
    val expected2 = AlterTableReplaceCol(
      tableIdent,
      None,
      StructType(Seq(
        StructField("new_col1", IntegerType, nullable = true, meta1),
        StructField("new_col2", LongType, nullable = true, meta2))),
      restrict = true,
      cascade = false)(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("show databases") {
    val sql1 = "SHOW DATABASES"
    val sql2 = "SHOW DATABASES LIKE 'defau*'"
    val parsed1 = parser.parsePlan(sql1)
    val expected1 = ShowDatabasesCommand(None)
    val parsed2 = parser.parsePlan(sql2)
    val expected2 = ShowDatabasesCommand(Some("defau*"))
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("show tblproperties") {
    val parsed1 = parser.parsePlan("SHOW TBLPROPERTIES tab1")
    val expected1 = ShowTablePropertiesCommand(TableIdentifier("tab1", None), None)
    val parsed2 = parser.parsePlan("SHOW TBLPROPERTIES tab1('propKey1')")
    val expected2 = ShowTablePropertiesCommand(TableIdentifier("tab1", None), Some("propKey1"))
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("commands only available in HiveContext") {
    intercept[ParseException] {
      parser.parsePlan("DROP TABLE D1.T1")
    }
    intercept[ParseException] {
      parser.parsePlan("CREATE VIEW testView AS SELECT id FROM tab")
    }
    intercept[ParseException] {
      parser.parsePlan("ALTER VIEW testView AS SELECT id FROM tab")
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE EXTERNAL TABLE parquet_tab2(c1 INT, c2 STRING)
          |TBLPROPERTIES('prop1Key '= "prop1Val", ' `prop2Key` '= "prop2Val")
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan("SELECT TRANSFORM (key, value) USING 'cat' AS (tKey, tValue) FROM testData")
    }
  }
}
