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

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


// TODO: merge this with DDLSuite (SPARK-14441)
class DDLCommandSuite extends PlanTest {
  private lazy val parser = new SparkSqlParser(new SQLConf)

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parser.parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase.contains("operation not allowed"))
    containsThesePhrases.foreach { p => assert(e.getMessage.toLowerCase.contains(p.toLowerCase)) }
  }

  private def parseAs[T: ClassTag](query: String): T = {
    parser.parsePlan(query) match {
      case t: T => t
      case other =>
        fail(s"Expected to parse ${classTag[T].runtimeClass} from query," +
          s"got ${other.getClass.getName}: $query")
    }
  }

  test("create database") {
    val sql =
      """
       |CREATE DATABASE IF NOT EXISTS database_name
       |COMMENT 'database_comment' LOCATION '/home/user/db'
       |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = CreateDatabaseCommand(
      "database_name",
      ifNotExists = true,
      Some("/home/user/db"),
      Some("database_comment"),
      Map("a" -> "a", "b" -> "b", "c" -> "c"))
    comparePlans(parsed, expected)
  }

  test("create database - property values must be set") {
    assertUnsupported(
      sql = "CREATE DATABASE my_db WITH DBPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
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

    val expected1 = DropDatabaseCommand(
      "database_name",
      ifExists = true,
      cascade = false)
    val expected2 = DropDatabaseCommand(
      "database_name",
      ifExists = true,
      cascade = true)
    val expected3 = DropDatabaseCommand(
      "database_name",
      ifExists = false,
      cascade = false)
    val expected4 = DropDatabaseCommand(
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

    val expected1 = AlterDatabasePropertiesCommand(
      "database_name",
      Map("a" -> "a", "b" -> "b", "c" -> "c"))
    val expected2 = AlterDatabasePropertiesCommand(
      "database_name",
      Map("a" -> "a"))

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter database - property values must be set") {
    assertUnsupported(
      sql = "ALTER DATABASE my_db SET DBPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("describe database") {
    // DESCRIBE DATABASE [EXTENDED] db_name;
    val sql1 = "DESCRIBE DATABASE EXTENDED db_name"
    val sql2 = "DESCRIBE DATABASE db_name"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = DescribeDatabaseCommand(
      "db_name",
      extended = true)
    val expected2 = DescribeDatabaseCommand(
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
    val expected1 = CreateFunctionCommand(
      None,
      "helloworld",
      "com.matthewrathbone.example.SimpleUDFExample",
      Seq(
        FunctionResource(FunctionResourceType.fromString("jar"), "/path/to/jar1"),
        FunctionResource(FunctionResourceType.fromString("jar"), "/path/to/jar2")),
      isTemp = true)
    val expected2 = CreateFunctionCommand(
      Some("hello"),
      "world",
      "com.matthewrathbone.example.SimpleUDFExample",
      Seq(
        FunctionResource(FunctionResourceType.fromString("archive"), "/path/to/archive"),
        FunctionResource(FunctionResourceType.fromString("file"), "/path/to/file")),
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

    val expected1 = DropFunctionCommand(
      None,
      "helloworld",
      ifExists = false,
      isTemp = true)
    val expected2 = DropFunctionCommand(
      None,
      "helloworld",
      ifExists = true,
      isTemp = true)
    val expected3 = DropFunctionCommand(
      Some("hello"),
      "world",
      ifExists = false,
      isTemp = false)
    val expected4 = DropFunctionCommand(
      Some("hello"),
      "world",
      ifExists = true,
      isTemp = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  test("create table - table file format") {
    val allSources = Seq("parquet", "parquetfile", "orc", "orcfile", "avro", "avrofile",
      "sequencefile", "rcfile", "textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab STORED AS $s"
      val ct = parseAs[CreateTable](query)
      val hiveSerde = HiveSerDe.sourceToSerDe(s)
      assert(hiveSerde.isDefined)
      assert(ct.tableDesc.storage.serde == hiveSerde.get.serde)
      assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
      assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
    }
  }

  test("create table - row format and table file format") {
    val createTableStart = "CREATE TABLE my_tab ROW FORMAT"
    val fileFormat = s"STORED AS INPUTFORMAT 'inputfmt' OUTPUTFORMAT 'outputfmt'"
    val query1 = s"$createTableStart SERDE 'anything' $fileFormat"
    val query2 = s"$createTableStart DELIMITED FIELDS TERMINATED BY ' ' $fileFormat"

    // No conflicting serdes here, OK
    val parsed1 = parseAs[CreateTable](query1)
    assert(parsed1.tableDesc.storage.serde == Some("anything"))
    assert(parsed1.tableDesc.storage.inputFormat == Some("inputfmt"))
    assert(parsed1.tableDesc.storage.outputFormat == Some("outputfmt"))
    val parsed2 = parseAs[CreateTable](query2)
    assert(parsed2.tableDesc.storage.serde.isEmpty)
    assert(parsed2.tableDesc.storage.inputFormat == Some("inputfmt"))
    assert(parsed2.tableDesc.storage.outputFormat == Some("outputfmt"))
  }

  test("create table - row format serde and generic file format") {
    val allSources = Seq("parquet", "orc", "avro", "sequencefile", "rcfile", "textfile")
    val supportedSources = Set("sequencefile", "rcfile", "textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab ROW FORMAT SERDE 'anything' STORED AS $s"
      if (supportedSources.contains(s)) {
        val ct = parseAs[CreateTable](query)
        val hiveSerde = HiveSerDe.sourceToSerDe(s)
        assert(hiveSerde.isDefined)
        assert(ct.tableDesc.storage.serde == Some("anything"))
        assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
        assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
      } else {
        assertUnsupported(query, Seq("row format serde", "incompatible", s))
      }
    }
  }

  test("create table - row format delimited and generic file format") {
    val allSources = Seq("parquet", "orc", "avro", "sequencefile", "rcfile", "textfile")
    val supportedSources = Set("textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS $s"
      if (supportedSources.contains(s)) {
        val ct = parseAs[CreateTable](query)
        val hiveSerde = HiveSerDe.sourceToSerDe(s)
        assert(hiveSerde.isDefined)
        assert(ct.tableDesc.storage.serde == hiveSerde.get.serde)
        assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
        assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
      } else {
        assertUnsupported(query, Seq("row format delimited", "only compatible with 'textfile'", s))
      }
    }
  }

  test("create external table - location must be specified") {
    assertUnsupported(
      sql = "CREATE EXTERNAL TABLE my_tab",
      containsThesePhrases = Seq("create external table", "location"))
    val query = "CREATE EXTERNAL TABLE my_tab LOCATION '/something/anything'"
    val ct = parseAs[CreateTable](query)
    assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
    assert(ct.tableDesc.storage.locationUri == Some("/something/anything"))
  }

  test("create table - property values must be set") {
    assertUnsupported(
      sql = "CREATE TABLE my_tab TBLPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
    assertUnsupported(
      sql = "CREATE TABLE my_tab ROW FORMAT SERDE 'serde' " +
        "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("create table - location implies external") {
    val query = "CREATE TABLE my_tab LOCATION '/something/anything'"
    val ct = parseAs[CreateTable](query)
    assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
    assert(ct.tableDesc.storage.locationUri == Some("/something/anything"))
  }

  test("create table using - with partitioned by") {
    val query = "CREATE TABLE my_tab(a INT comment 'test', b STRING) " +
      "USING parquet PARTITIONED BY (a)"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", IntegerType, nullable = true, "test")
        .add("b", StringType),
      provider = Some("parquet"),
      partitionColumnNames = Seq("a")
    )

    parser.parsePlan(query) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
          s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table using - with bucket") {
    val query = "CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
      "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"),
      bucketSpec = Some(BucketSpec(5, Seq("a"), Seq("b")))
    )

    parser.parsePlan(query) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
          s"got ${other.getClass.getName}: $query")
    }
  }

  // ALTER TABLE table_name RENAME TO new_table_name;
  // ALTER VIEW view_name RENAME TO new_view_name;
  test("alter table/view: rename table/view") {
    val sql_table = "ALTER TABLE table_name RENAME TO new_table_name"
    val sql_view = sql_table.replace("TABLE", "VIEW")
    val parsed_table = parser.parsePlan(sql_table)
    val parsed_view = parser.parsePlan(sql_view)
    val expected_table = AlterTableRenameCommand(
      TableIdentifier("table_name", None),
      "new_table_name",
      isView = false)
    val expected_view = AlterTableRenameCommand(
      TableIdentifier("table_name", None),
      "new_table_name",
      isView = true)
    comparePlans(parsed_table, expected_table)
    comparePlans(parsed_view, expected_view)

    val e = intercept[ParseException](
      parser.parsePlan("ALTER TABLE db1.tbl RENAME TO db1.tbl2")
    )
    assert(e.getMessage.contains("Can not specify database in table/view name after RENAME TO"))
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
    val expected1_table = AlterTableSetPropertiesCommand(
      tableIdent, Map("test" -> "test", "comment" -> "new_comment"), isView = false)
    val expected2_table = AlterTableUnsetPropertiesCommand(
      tableIdent, Seq("comment", "test"), ifExists = false, isView = false)
    val expected3_table = AlterTableUnsetPropertiesCommand(
      tableIdent, Seq("comment", "test"), ifExists = true, isView = false)
    val expected1_view = expected1_table.copy(isView = true)
    val expected2_view = expected2_table.copy(isView = true)
    val expected3_view = expected3_table.copy(isView = true)

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
    comparePlans(parsed3_table, expected3_table)
    comparePlans(parsed1_view, expected1_view)
    comparePlans(parsed2_view, expected2_view)
    comparePlans(parsed3_view, expected3_view)
  }

  test("alter table - property values must be set") {
    assertUnsupported(
      sql = "ALTER TABLE my_tab SET TBLPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("alter table unset properties - property values must NOT be set") {
    assertUnsupported(
      sql = "ALTER TABLE my_tab UNSET TBLPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_with_value"))
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
    val expected1 = AlterTableSerDePropertiesCommand(
      tableIdent, Some("org.apache.class"), None, None)
    val expected2 = AlterTableSerDePropertiesCommand(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    val expected3 = AlterTableSerDePropertiesCommand(
      tableIdent, None, Some(Map("columns" -> "foo,bar", "field.delim" -> ",")), None)
    val expected4 = AlterTableSerDePropertiesCommand(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> null, "dt" -> "2008-08-08", "country" -> "us")))
    val expected5 = AlterTableSerDePropertiesCommand(
      tableIdent,
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> null, "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
    comparePlans(parsed5, expected5)
  }

  test("alter table - SerDe property values must be set") {
    assertUnsupported(
      sql = "ALTER TABLE my_tab SET SERDE 'serde' " +
        "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
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

    val expected1 = AlterTableAddPartitionCommand(
      TableIdentifier("table_name", None),
      Seq(
        (Map("dt" -> "2008-08-08", "country" -> "us"), Some("location1")),
        (Map("dt" -> "2009-09-09", "country" -> "uk"), None)),
      ifNotExists = true)
    val expected2 = AlterTableAddPartitionCommand(
      TableIdentifier("table_name", None),
      Seq((Map("dt" -> "2008-08-08"), Some("loc"))),
      ifNotExists = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: recover partitions") {
    val sql = "ALTER TABLE table_name RECOVER PARTITIONS"
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableRecoverPartitionsCommand(
      TableIdentifier("table_name", None))
    comparePlans(parsed, expected)
  }

  test("alter view: add partition (not supported)") {
    assertUnsupported(
      """
        |ALTER VIEW view_name ADD IF NOT EXISTS PARTITION
        |(dt='2008-08-08', country='us') PARTITION
        |(dt='2009-09-09', country='uk')
      """.stripMargin)
  }

  test("alter table: rename partition") {
    val sql =
      """
       |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
       |RENAME TO PARTITION (dt='2008-09-09', country='uk')
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableRenamePartitionCommand(
      TableIdentifier("table_name", None),
      Map("dt" -> "2008-08-08", "country" -> "us"),
      Map("dt" -> "2008-09-09", "country" -> "uk"))
    comparePlans(parsed, expected)
  }

  test("alter table: exchange partition (not supported)") {
    assertUnsupported(
      """
       |ALTER TABLE table_name_1 EXCHANGE PARTITION
       |(dt='2008-08-08', country='us') WITH TABLE table_name_2
      """.stripMargin)
  }

  // ALTER TABLE table_name DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...]
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
       |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val sql1_view = sql1_table.replace("TABLE", "VIEW")
    val sql2_view = sql2_table.replace("TABLE", "VIEW")

    val parsed1_table = parser.parsePlan(sql1_table)
    val parsed2_table = parser.parsePlan(sql2_table)
    val parsed1_purge = parser.parsePlan(sql1_table + " PURGE")
    assertUnsupported(sql1_view)
    assertUnsupported(sql2_view)

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_table = AlterTableDropPartitionCommand(
      tableIdent,
      Seq(
        Map("dt" -> ("=", "2008-08-08"), "country" -> ("=", "us")),
        Map("dt" -> ("=", "2009-09-09"), "country" -> ("=", "uk"))),
      ifExists = true,
      purge = false)
    val expected2_table = expected1_table.copy(ifExists = false)
    val expected1_purge = expected1_table.copy(purge = true)

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
    comparePlans(parsed1_purge, expected1_purge)
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

  test("alter table: set location") {
    val sql1 = "ALTER TABLE table_name SET LOCATION 'new location'"
    val sql2 = "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "SET LOCATION 'new location'"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSetLocationCommand(
      tableIdent,
      None,
      "new location")
    val expected2 = AlterTableSetLocationCommand(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      "new location")
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
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

  test("alter table: cluster by (not supported)") {
    assertUnsupported(
      "ALTER TABLE table_name CLUSTERED BY (col_name) SORTED BY (col2_name) INTO 3 BUCKETS")
    assertUnsupported("ALTER TABLE table_name CLUSTERED BY (col_name) INTO 3 BUCKETS")
    assertUnsupported("ALTER TABLE table_name NOT CLUSTERED")
    assertUnsupported("ALTER TABLE table_name NOT SORTED")
  }

  test("alter table: skewed by (not supported)") {
    assertUnsupported("ALTER TABLE table_name NOT SKEWED")
    assertUnsupported("ALTER TABLE table_name NOT STORED AS DIRECTORIES")
    assertUnsupported("ALTER TABLE table_name SET SKEWED LOCATION (col_name1=\"location1\"")
    assertUnsupported("ALTER TABLE table_name SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES")
  }

  test("alter table: change column name/type/position/comment (not allowed)") {
    assertUnsupported("ALTER TABLE table_name CHANGE col_old_name col_new_name INT")
    assertUnsupported(
      """
       |ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT
       |COMMENT 'col_comment' FIRST CASCADE
      """.stripMargin)
    assertUnsupported("""
       |ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT
       |COMMENT 'col_comment' AFTER column_name RESTRICT
      """.stripMargin)
  }

  test("alter table: add/replace columns (not allowed)") {
    assertUnsupported(
      """
       |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
       |ADD COLUMNS (new_col1 INT COMMENT 'test_comment', new_col2 LONG
       |COMMENT 'test_comment2') CASCADE
      """.stripMargin)
    assertUnsupported(
      """
       |ALTER TABLE table_name REPLACE COLUMNS (new_col1 INT
       |COMMENT 'test_comment', new_col2 LONG COMMENT 'test_comment2') RESTRICT
      """.stripMargin)
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

  test("SPARK-14383: DISTRIBUTE and UNSET as non-keywords") {
    val sql = "SELECT distribute, unset FROM x"
    val parsed = parser.parsePlan(sql)
    assert(parsed.isInstanceOf[Project])
  }

  test("duplicate keys in table properties") {
    val e = intercept[ParseException] {
      parser.parsePlan("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('key1' = '1', 'key1' = '2')")
    }.getMessage
    assert(e.contains("Found duplicate keys 'key1'"))
  }

  test("duplicate columns in partition specs") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        "ALTER TABLE dbx.tab1 PARTITION (a='1', a='2') RENAME TO PARTITION (a='100', a='200')")
    }.getMessage
    assert(e.contains("Found duplicate keys 'a'"))
  }

  test("drop table") {
    val tableName1 = "db.tab"
    val tableName2 = "tab"

    val parsed = Seq(
        s"DROP TABLE $tableName1",
        s"DROP TABLE IF EXISTS $tableName1",
        s"DROP TABLE $tableName2",
        s"DROP TABLE IF EXISTS $tableName2",
        s"DROP TABLE $tableName2 PURGE",
        s"DROP TABLE IF EXISTS $tableName2 PURGE"
      ).map(parser.parsePlan)

    val expected = Seq(
      DropTableCommand(TableIdentifier("tab", Option("db")), ifExists = false, isView = false,
        purge = false),
      DropTableCommand(TableIdentifier("tab", Option("db")), ifExists = true, isView = false,
        purge = false),
      DropTableCommand(TableIdentifier("tab", None), ifExists = false, isView = false,
        purge = false),
      DropTableCommand(TableIdentifier("tab", None), ifExists = true, isView = false,
        purge = false),
      DropTableCommand(TableIdentifier("tab", None), ifExists = false, isView = false,
        purge = true),
      DropTableCommand(TableIdentifier("tab", None), ifExists = true, isView = false,
        purge = true))

    parsed.zip(expected).foreach { case (p, e) => comparePlans(p, e) }
  }

  test("drop view") {
    val viewName1 = "db.view"
    val viewName2 = "view"

    val parsed1 = parser.parsePlan(s"DROP VIEW $viewName1")
    val parsed2 = parser.parsePlan(s"DROP VIEW IF EXISTS $viewName1")
    val parsed3 = parser.parsePlan(s"DROP VIEW $viewName2")
    val parsed4 = parser.parsePlan(s"DROP VIEW IF EXISTS $viewName2")

    val expected1 =
      DropTableCommand(TableIdentifier("view", Option("db")), ifExists = false, isView = true,
        purge = false)
    val expected2 =
      DropTableCommand(TableIdentifier("view", Option("db")), ifExists = true, isView = true,
        purge = false)
    val expected3 =
      DropTableCommand(TableIdentifier("view", None), ifExists = false, isView = true,
        purge = false)
    val expected4 =
      DropTableCommand(TableIdentifier("view", None), ifExists = true, isView = true,
        purge = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  test("show columns") {
    val sql1 = "SHOW COLUMNS FROM t1"
    val sql2 = "SHOW COLUMNS IN db1.t1"
    val sql3 = "SHOW COLUMNS FROM t1 IN db1"
    val sql4 = "SHOW COLUMNS FROM db1.t1 IN db1"
    val sql5 = "SHOW COLUMNS FROM db1.t1 IN db2"

    val parsed1 = parser.parsePlan(sql1)
    val expected1 = ShowColumnsCommand(TableIdentifier("t1", None))
    val parsed2 = parser.parsePlan(sql2)
    val expected2 = ShowColumnsCommand(TableIdentifier("t1", Some("db1")))
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql3)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected2)
    comparePlans(parsed4, expected2)
    assertUnsupported(sql5)
  }

  test("show partitions") {
    val sql1 = "SHOW PARTITIONS t1"
    val sql2 = "SHOW PARTITIONS db1.t1"
    val sql3 = "SHOW PARTITIONS t1 PARTITION(partcol1='partvalue', partcol2='partvalue')"

    val parsed1 = parser.parsePlan(sql1)
    val expected1 =
      ShowPartitionsCommand(TableIdentifier("t1", None), None)
    val parsed2 = parser.parsePlan(sql2)
    val expected2 =
      ShowPartitionsCommand(TableIdentifier("t1", Some("db1")), None)
    val expected3 =
      ShowPartitionsCommand(TableIdentifier("t1", None),
        Some(Map("partcol1" -> "partvalue", "partcol2" -> "partvalue")))
    val parsed3 = parser.parsePlan(sql3)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
  }

  test("support for other types in DBPROPERTIES") {
    val sql =
      """
        |CREATE DATABASE database_name
        |LOCATION '/home/user/db'
        |WITH DBPROPERTIES ('a'=1, 'b'=0.1, 'c'=TRUE)
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = CreateDatabaseCommand(
      "database_name",
      ifNotExists = false,
      Some("/home/user/db"),
      None,
      Map("a" -> "1", "b" -> "0.1", "c" -> "true"))

    comparePlans(parsed, expected)
  }

  test("support for other types in TBLPROPERTIES") {
    val sql =
      """
        |ALTER TABLE table_name
        |SET TBLPROPERTIES ('a' = 1, 'b' = 0.1, 'c' = TRUE)
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableSetPropertiesCommand(
      TableIdentifier("table_name"),
      Map("a" -> "1", "b" -> "0.1", "c" -> "true"),
      isView = false)

    comparePlans(parsed, expected)
  }

  test("support for other types in OPTIONS") {
    val sql =
      """
        |CREATE TABLE table_name USING json
        |OPTIONS (a 1, b 0.1, c TRUE)
      """.stripMargin

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("table_name"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        properties = Map("a" -> "1", "b" -> "0.1", "c" -> "true")
      ),
      schema = new StructType,
      provider = Some("json")
    )

    parser.parsePlan(sql) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
          s"got ${other.getClass.getName}: $sql")
    }
  }
}
