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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.execution.SparkQl
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.types._

class DDLCommandSuite extends PlanTest {
  private val parser = new SparkQl

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
      Map("a" -> "a", "b" -> "b", "c" -> "c"))(sql)
    comparePlans(parsed, expected)
  }

  test("create function") {
    val sql1 =
      """
       |CREATE TEMPORARY FUNCTION helloworld as
       |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar',
       |FILE 'path/to/file'
     """.stripMargin
    val sql2 =
      """
        |CREATE FUNCTION hello.world as
        |'com.matthewrathbone.example.SimpleUDFExample' USING ARCHIVE '/path/to/archive',
        |FILE 'path/to/file'
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val expected1 = CreateFunction(
      "helloworld",
      "com.matthewrathbone.example.SimpleUDFExample",
      Map("jar" -> "/path/to/jar", "file" -> "path/to/file"),
      isTemp = true)(sql1)
    val expected2 = CreateFunction(
      "hello.world",
      "com.matthewrathbone.example.SimpleUDFExample",
      Map("archive" -> "/path/to/archive", "file" -> "path/to/file"),
      isTemp = false)(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: rename table") {
    val sql = "ALTER TABLE table_name RENAME TO new_table_name"
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableRename(
      TableIdentifier("table_name", None),
      TableIdentifier("new_table_name", None))(sql)
    comparePlans(parsed, expected)
  }

  test("alter table: alter table properties") {
    val sql1 = "ALTER TABLE table_name SET TBLPROPERTIES ('test' = 'test', " +
      "'comment' = 'new_comment')"
    val sql2 = "ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3 = "ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSetProperties(
      tableIdent, Map("test" -> "test", "comment" -> "new_comment"))(sql1)
    val expected2 = AlterTableUnsetProperties(
      tableIdent, Map("comment" -> null, "test" -> null), ifExists = false)(sql2)
    val expected3 = AlterTableUnsetProperties(
      tableIdent, Map("comment" -> null, "test" -> null), ifExists = true)(sql3)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
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

  test("alter table: add partition") {
    val sql =
      """
       |ALTER TABLE table_name ADD IF NOT EXISTS PARTITION
       |(dt='2008-08-08', country='us') LOCATION 'location1' PARTITION
       |(dt='2009-09-09', country='uk')
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableAddPartition(
      TableIdentifier("table_name", None),
      Seq(
        (Map("dt" -> "2008-08-08", "country" -> "us"), Some("location1")),
        (Map("dt" -> "2009-09-09", "country" -> "uk"), None)),
      ifNotExists = true)(sql)
    comparePlans(parsed, expected)
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

  test("alter table: drop partitions") {
    val sql1 =
      """
       |ALTER TABLE table_name DROP IF EXISTS PARTITION
       |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val sql2 =
      """
       |ALTER TABLE table_name DROP PARTITION
       |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk') PURGE
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = true,
      purge = false)(sql1)
    val expected2 = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = false,
      purge = true)(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
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

}
