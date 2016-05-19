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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Project, ScriptTransformation}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.{BucketSpec, CreateTableUsing}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// TODO: merge this with DDLSuite (SPARK-14441)
class PlanParserSuite extends PlanTest {
  private val parser = new SparkSqlParser(new SQLConf)

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parser.parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase.contains("operation not allowed"))
    containsThesePhrases.foreach { p => assert(e.getMessage.toLowerCase.contains(p.toLowerCase)) }
  }

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parser.parsePlan(sql).collect {
      case CreateTable(desc, allowExisting) => (desc, allowExisting)
      case CreateTableAsSelectLogicalPlan(desc, _, allowExisting) => (desc, allowExisting)
      case CreateViewCommand(desc, _, allowExisting, _, _, _) => (desc, allowExisting)
    }.head
  }

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
      Seq(
        FunctionResource(FunctionResourceType.fromString("jar"), "/path/to/jar1"),
        FunctionResource(FunctionResourceType.fromString("jar"), "/path/to/jar2")),
      isTemp = true)
    val expected2 = CreateFunction(
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

  test("create external table - location must be specified") {
    assertUnsupported(
      sql = "CREATE EXTERNAL TABLE my_tab",
      containsThesePhrases = Seq("create external table", "location"))
    val query = "CREATE EXTERNAL TABLE my_tab LOCATION '/something/anything'"
    parser.parsePlan(query) match {
      case ct: CreateTable =>
        assert(ct.table.tableType == CatalogTableType.EXTERNAL)
        assert(ct.table.storage.locationUri == Some("/something/anything"))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTable].getClass.getName} from query," +
          s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - location implies external") {
    val query = "CREATE TABLE my_tab LOCATION '/something/anything'"
    parser.parsePlan(query) match {
      case ct: CreateTable =>
        assert(ct.table.tableType == CatalogTableType.EXTERNAL)
        assert(ct.table.storage.locationUri == Some("/something/anything"))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTable].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table using - with partitioned by") {
    val query = "CREATE TABLE my_tab(a INT, b STRING) USING parquet PARTITIONED BY (a)"
    val expected = CreateTableUsing(
      TableIdentifier("my_tab"),
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      "parquet",
      false,
      Map.empty,
      null,
      None,
      false,
      true)

    parser.parsePlan(query) match {
      case ct: CreateTableUsing =>
        // We can't compare array in `CreateTableUsing` directly, so here we compare
        // `partitionColumns` ahead, and make `partitionColumns` null before plan comparison.
        assert(Seq("a") == ct.partitionColumns.toSeq)
        comparePlans(ct.copy(partitionColumns = null), expected)
      case other =>
        fail(s"Expected to parse ${classOf[CreateTable].getClass.getName} from query," +
          s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table using - with bucket") {
    val query = "CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
      "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"
    val expected = CreateTableUsing(
      TableIdentifier("my_tab"),
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      "parquet",
      false,
      Map.empty,
      null,
      Some(BucketSpec(5, Seq("a"), Seq("b"))),
      false,
      true)

    parser.parsePlan(query) match {
      case ct: CreateTableUsing =>
        // `Array.empty == Array.empty` returns false, here we set `partitionColumns` to null before
        // plan comparison.
        assert(ct.partitionColumns.isEmpty)
        comparePlans(ct.copy(partitionColumns = null), expected)
      case other =>
        fail(s"Expected to parse ${classOf[CreateTable].getClass.getName} from query," +
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
    val expected_table = AlterTableRename(
      TableIdentifier("table_name", None),
      TableIdentifier("new_table_name", None),
      isView = false)
    val expected_view = AlterTableRename(
      TableIdentifier("table_name", None),
      TableIdentifier("new_table_name", None),
      isView = true)
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
      tableIdent, Map("test" -> "test", "comment" -> "new_comment"), isView = false)
    val expected2_table = AlterTableUnsetProperties(
      tableIdent, Seq("comment", "test"), ifExists = false, isView = false)
    val expected3_table = AlterTableUnsetProperties(
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
      tableIdent, Some("org.apache.class"), None, None)
    val expected2 = AlterTableSerDeProperties(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    val expected3 = AlterTableSerDeProperties(
      tableIdent, None, Some(Map("columns" -> "foo,bar", "field.delim" -> ",")), None)
    val expected4 = AlterTableSerDeProperties(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> null, "dt" -> "2008-08-08", "country" -> "us")))
    val expected5 = AlterTableSerDeProperties(
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
      ifNotExists = true)
    val expected2 = AlterTableAddPartition(
      TableIdentifier("table_name", None),
      Seq((Map("dt" -> "2008-08-08"), Some("loc"))),
      ifNotExists = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
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
    val expected = AlterTableRenamePartition(
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
    assertUnsupported(sql1_table + " PURGE")
    assertUnsupported(sql2_table + " PURGE")
    assertUnsupported(sql1_view)
    assertUnsupported(sql2_view)

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_table = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = true)
    val expected2_table = expected1_table.copy(ifExists = false)

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
  }

  test("alter table: archive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name ARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: unarchive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name UNARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: set file format (not allowed)") {
    assertUnsupported(
      "ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test' " +
        "OUTPUTFORMAT 'test' SERDE 'test'")
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
    val expected1 = AlterTableSetLocation(
      tableIdent,
      None,
      "new location")
    val expected2 = AlterTableSetLocation(
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

  test("drop table") {
    val tableName1 = "db.tab"
    val tableName2 = "tab"

    val parsed1 = parser.parsePlan(s"DROP TABLE $tableName1")
    val parsed2 = parser.parsePlan(s"DROP TABLE IF EXISTS $tableName1")
    val parsed3 = parser.parsePlan(s"DROP TABLE $tableName2")
    val parsed4 = parser.parsePlan(s"DROP TABLE IF EXISTS $tableName2")
    assertUnsupported(s"DROP TABLE IF EXISTS $tableName2 PURGE")

    val expected1 =
      DropTable(TableIdentifier("tab", Option("db")), ifExists = false, isView = false)
    val expected2 =
      DropTable(TableIdentifier("tab", Option("db")), ifExists = true, isView = false)
    val expected3 =
      DropTable(TableIdentifier("tab", None), ifExists = false, isView = false)
    val expected4 =
      DropTable(TableIdentifier("tab", None), ifExists = true, isView = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  test("drop view") {
    val viewName1 = "db.view"
    val viewName2 = "view"

    val parsed1 = parser.parsePlan(s"DROP VIEW $viewName1")
    val parsed2 = parser.parsePlan(s"DROP VIEW IF EXISTS $viewName1")
    val parsed3 = parser.parsePlan(s"DROP VIEW $viewName2")
    val parsed4 = parser.parsePlan(s"DROP VIEW IF EXISTS $viewName2")

    val expected1 =
      DropTable(TableIdentifier("view", Option("db")), ifExists = false, isView = true)
    val expected2 =
      DropTable(TableIdentifier("view", Option("db")), ifExists = true, isView = true)
    val expected3 =
      DropTable(TableIdentifier("view", None), ifExists = false, isView = true)
    val expected4 =
      DropTable(TableIdentifier("view", None), ifExists = true, isView = true)

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

  test("Test CTAS #2") {
    val s2 =
      """CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |(viewTime INT,
        |userid BIGINT,
        |page_url STRING,
        |referrer_url STRING,
        |ip STRING COMMENT 'IP Address of the User',
        |country STRING COMMENT 'country of origination')
        |COMMENT 'This is the staging page view table'
        |PARTITIONED BY (dt STRING COMMENT 'date type', hour STRING COMMENT 'hour of the day')
        |ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
        | OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src""".stripMargin

    val (desc, exists) = extractTableDesc(s2)
    assert(exists)
    assert(desc.identifier.database == Some("mydb"))
    assert(desc.identifier.table == "page_view")
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.storage.locationUri == Some("/user/external/page_view"))
    assert(desc.schema ==
      CatalogColumn("viewtime", "int") ::
        CatalogColumn("userid", "bigint") ::
        CatalogColumn("page_url", "string") ::
        CatalogColumn("referrer_url", "string") ::
        CatalogColumn("ip", "string", comment = Some("IP Address of the User")) ::
        CatalogColumn("country", "string", comment = Some("country of origination")) ::
        CatalogColumn("dt", "string", comment = Some("date type")) ::
        CatalogColumn("hour", "string", comment = Some("hour of the day")) :: Nil)
    // TODO will be SQLText
    assert(desc.comment == Some("This is the staging page view table"))
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.partitionColumns ==
      CatalogColumn("dt", "string", comment = Some("date type")) ::
        CatalogColumn("hour", "string", comment = Some("hour of the day")) :: Nil)
    assert(desc.storage.serdeProperties == Map())
    assert(desc.storage.inputFormat == Some("parquet.hive.DeprecatedParquetInputFormat"))
    assert(desc.storage.outputFormat == Some("parquet.hive.DeprecatedParquetOutputFormat"))
    assert(desc.storage.serde == Some("parquet.hive.serde.ParquetHiveSerDe"))
    assert(desc.properties == Map(("p1", "v1"), ("p2", "v2")))
  }

  test("Test CTAS #3") {
    val s3 = """CREATE TABLE page_view AS SELECT * FROM src"""
    val (desc, exists) = extractTableDesc(s3)
    assert(exists == false)
    assert(desc.identifier.database == None)
    assert(desc.identifier.table == "page_view")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.storage.locationUri == None)
    assert(desc.schema == Seq.empty[CatalogColumn])
    assert(desc.viewText == None) // TODO will be SQLText
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.serdeProperties == Map())
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde.isEmpty)
    assert(desc.properties == Map())
  }

  test("Test CTAS #4") {
    val s4 =
      """CREATE TABLE page_view
        |STORED BY 'storage.handler.class.name' AS SELECT * FROM src""".stripMargin
    intercept[AnalysisException] {
      extractTableDesc(s4)
    }
  }

  test("Test CTAS #5") {
    val s5 = """CREATE TABLE ctas2
               | ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
               | WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
               | STORED AS RCFile
               | TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
               | AS
               |   SELECT key, value
               |   FROM src
               |   ORDER BY key, value""".stripMargin
    val (desc, exists) = extractTableDesc(s5)
    assert(exists == false)
    assert(desc.identifier.database == None)
    assert(desc.identifier.table == "ctas2")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.storage.locationUri == None)
    assert(desc.schema == Seq.empty[CatalogColumn])
    assert(desc.viewText == None) // TODO will be SQLText
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.serdeProperties == Map(("serde_p1" -> "p1"), ("serde_p2" -> "p2")))
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
    assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
    assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"))
    assert(desc.properties == Map(("tbl_p1" -> "p11"), ("tbl_p2" -> "p22")))
  }

  test("unsupported operations") {
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE TEMPORARY TABLE ctas2
          |ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
          |WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
          |STORED AS RCFile
          |TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
          |AS SELECT key, value FROM src ORDER BY key, value
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
          |CLUSTERED BY(user_id) INTO 256 BUCKETS
          |AS SELECT key, value FROM src ORDER BY key, value
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
          |SKEWED BY (key) ON (1,5,6)
          |AS SELECT key, value FROM src ORDER BY key, value
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |SELECT TRANSFORM (key, value) USING 'cat' AS (tKey, tValue)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe'
          |RECORDREADER 'org.apache.hadoop.hive.contrib.util.typedbytes.TypedBytesRecordReader'
          |FROM testData
        """.stripMargin)
    }
  }

  test("Invalid interval term should throw AnalysisException") {
    def assertError(sql: String, errorMessage: String): Unit = {
      val e = intercept[AnalysisException] {
        parser.parsePlan(sql)
      }
      assert(e.getMessage.contains(errorMessage))
    }
    assertError("select interval '42-32' year to month",
      "month 32 outside range [0, 11]")
    assertError("select interval '5 49:12:15' day to second",
      "hour 49 outside range [0, 23]")
    assertError("select interval '.1111111111' second",
      "nanosecond 1111111111 outside range")
  }

  test("transform query spec") {
    val plan1 = parser.parsePlan("select transform(a, b) using 'func' from e where f < 10")
      .asInstanceOf[ScriptTransformation].copy(ioschema = null)
    val plan2 = parser.parsePlan("map a, b using 'func' as c, d from e")
      .asInstanceOf[ScriptTransformation].copy(ioschema = null)
    val plan3 = parser.parsePlan("reduce a, b using 'func' as (c: int, d decimal(10, 0)) from e")
      .asInstanceOf[ScriptTransformation].copy(ioschema = null)

    val p = ScriptTransformation(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")),
      "func", Seq.empty, plans.table("e"), null)

    comparePlans(plan1,
      p.copy(child = p.child.where('f < 10), output = Seq('key.string, 'value.string)))
    comparePlans(plan2,
      p.copy(output = Seq('c.string, 'd.string)))
    comparePlans(plan3,
      p.copy(output = Seq('c.int, 'd.decimal(10, 0))))
  }

  test("use backticks in output of Script Transform") {
    parser.parsePlan(
      """SELECT `t`.`thing1`
        |FROM (SELECT TRANSFORM (`parquet_t1`.`key`, `parquet_t1`.`value`)
        |USING 'cat' AS (`thing1` int, `thing2` string) FROM `default`.`parquet_t1`) AS t
      """.stripMargin)
  }

  test("use backticks in output of Generator") {
    parser.parsePlan(
      """
        |SELECT `gentab2`.`gencol2`
        |FROM `default`.`src`
        |LATERAL VIEW explode(array(array(1, 2, 3))) `gentab1` AS `gencol1`
        |LATERAL VIEW explode(`gentab1`.`gencol1`) `gentab2` AS `gencol2`
      """.stripMargin)
  }

  test("use escaped backticks in output of Generator") {
    parser.parsePlan(
      """
        |SELECT `gen``tab2`.`gen``col2`
        |FROM `default`.`src`
        |LATERAL VIEW explode(array(array(1, 2,  3))) `gen``tab1` AS `gen``col1`
        |LATERAL VIEW explode(`gen``tab1`.`gen``col1`) `gen``tab2` AS `gen``col2`
      """.stripMargin)
  }

  test("create table - basic") {
    val query = "CREATE TABLE my_table (id int, name string)"
    val (desc, allowExisting) = extractTableDesc(query)
    assert(!allowExisting)
    assert(desc.identifier.database.isEmpty)
    assert(desc.identifier.table == "my_table")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.schema == Seq(CatalogColumn("id", "int"), CatalogColumn("name", "string")))
    assert(desc.partitionColumnNames.isEmpty)
    assert(desc.sortColumnNames.isEmpty)
    assert(desc.bucketColumnNames.isEmpty)
    assert(desc.numBuckets == -1)
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.locationUri.isEmpty)
    assert(desc.storage.inputFormat ==
      Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde.isEmpty)
    assert(desc.storage.serdeProperties.isEmpty)
    assert(desc.properties.isEmpty)
    assert(desc.comment.isEmpty)
  }

  test("create table - with database name") {
    val query = "CREATE TABLE dbx.my_table (id int, name string)"
    val (desc, _) = extractTableDesc(query)
    assert(desc.identifier.database == Some("dbx"))
    assert(desc.identifier.table == "my_table")
  }

  test("create table - temporary") {
    val query = "CREATE TEMPORARY TABLE tab1 (id int, name string)"
    val e = intercept[ParseException] { parser.parsePlan(query) }
    assert(e.message.contains("CREATE TEMPORARY TABLE is not supported yet"))
  }

  test("create table - external") {
    val query = "CREATE EXTERNAL TABLE tab1 (id int, name string) LOCATION '/path/to/nowhere'"
    val (desc, _) = extractTableDesc(query)
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.storage.locationUri == Some("/path/to/nowhere"))
  }

  test("create table - if not exists") {
    val query = "CREATE TABLE IF NOT EXISTS tab1 (id int, name string)"
    val (_, allowExisting) = extractTableDesc(query)
    assert(allowExisting)
  }

  test("create table - comment") {
    val query = "CREATE TABLE my_table (id int, name string) COMMENT 'its hot as hell below'"
    val (desc, _) = extractTableDesc(query)
    assert(desc.comment == Some("its hot as hell below"))
  }

  test("create table - partitioned columns") {
    val query = "CREATE TABLE my_table (id int, name string) PARTITIONED BY (month int)"
    val (desc, _) = extractTableDesc(query)
    assert(desc.schema == Seq(
      CatalogColumn("id", "int"),
      CatalogColumn("name", "string"),
      CatalogColumn("month", "int")))
    assert(desc.partitionColumnNames == Seq("month"))
  }

  test("create table - clustered by") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) CLUSTERED BY(id)"
    val query1 = s"$baseQuery INTO 10 BUCKETS"
    val query2 = s"$baseQuery SORTED BY(id) INTO 10 BUCKETS"
    val e1 = intercept[ParseException] { parser.parsePlan(query1) }
    val e2 = intercept[ParseException] { parser.parsePlan(query2) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
  }

  test("create table - skewed by") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) SKEWED BY"
    val query1 = s"$baseQuery(id) ON (1, 10, 100)"
    val query2 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z'))"
    val query3 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z')) STORED AS DIRECTORIES"
    val e1 = intercept[ParseException] { parser.parsePlan(query1) }
    val e2 = intercept[ParseException] { parser.parsePlan(query2) }
    val e3 = intercept[ParseException] { parser.parsePlan(query3) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
    assert(e3.getMessage.contains("Operation not allowed"))
  }

  test("create table - row format") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) ROW FORMAT"
    val query1 = s"$baseQuery SERDE 'org.apache.poof.serde.Baff'"
    val query2 = s"$baseQuery SERDE 'org.apache.poof.serde.Baff' WITH SERDEPROPERTIES ('k1'='v1')"
    val query3 =
      s"""
         |$baseQuery DELIMITED FIELDS TERMINATED BY 'x' ESCAPED BY 'y'
         |COLLECTION ITEMS TERMINATED BY 'a'
         |MAP KEYS TERMINATED BY 'b'
         |LINES TERMINATED BY '\n'
         |NULL DEFINED AS 'c'
      """.stripMargin
    val (desc1, _) = extractTableDesc(query1)
    val (desc2, _) = extractTableDesc(query2)
    val (desc3, _) = extractTableDesc(query3)
    assert(desc1.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc1.storage.serdeProperties.isEmpty)
    assert(desc2.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc2.storage.serdeProperties == Map("k1" -> "v1"))
    assert(desc3.storage.serdeProperties == Map(
      "field.delim" -> "x",
      "escape.delim" -> "y",
      "serialization.format" -> "x",
      "line.delim" -> "\n",
      "colelction.delim" -> "a", // yes, it's a typo from Hive :)
      "mapkey.delim" -> "b"))
  }

  test("create table - file format") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) STORED AS"
    val query1 = s"$baseQuery INPUTFORMAT 'winput' OUTPUTFORMAT 'wowput'"
    val query2 = s"$baseQuery ORC"
    val (desc1, _) = extractTableDesc(query1)
    val (desc2, _) = extractTableDesc(query2)
    assert(desc1.storage.inputFormat == Some("winput"))
    assert(desc1.storage.outputFormat == Some("wowput"))
    assert(desc1.storage.serde.isEmpty)
    assert(desc2.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"))
    assert(desc2.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))
    assert(desc2.storage.serde == Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
  }

  test("create table - storage handler") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) STORED BY"
    val query1 = s"$baseQuery 'org.papachi.StorageHandler'"
    val query2 = s"$baseQuery 'org.mamachi.StorageHandler' WITH SERDEPROPERTIES ('k1'='v1')"
    val e1 = intercept[ParseException] { parser.parsePlan(query1) }
    val e2 = intercept[ParseException] { parser.parsePlan(query2) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
  }

  test("create table - properties") {
    val query = "CREATE TABLE my_table (id int, name string) TBLPROPERTIES ('k1'='v1', 'k2'='v2')"
    val (desc, _) = extractTableDesc(query)
    assert(desc.properties == Map("k1" -> "v1", "k2" -> "v2"))
  }

  test("create table - everything!") {
    val query =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS dbx.my_table (id int, name string)
        |COMMENT 'no comment'
        |PARTITIONED BY (month int)
        |ROW FORMAT SERDE 'org.apache.poof.serde.Baff' WITH SERDEPROPERTIES ('k1'='v1')
        |STORED AS INPUTFORMAT 'winput' OUTPUTFORMAT 'wowput'
        |LOCATION '/path/to/mercury'
        |TBLPROPERTIES ('k1'='v1', 'k2'='v2')
      """.stripMargin
    val (desc, allowExisting) = extractTableDesc(query)
    assert(allowExisting)
    assert(desc.identifier.database == Some("dbx"))
    assert(desc.identifier.table == "my_table")
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.schema == Seq(
      CatalogColumn("id", "int"),
      CatalogColumn("name", "string"),
      CatalogColumn("month", "int")))
    assert(desc.partitionColumnNames == Seq("month"))
    assert(desc.sortColumnNames.isEmpty)
    assert(desc.bucketColumnNames.isEmpty)
    assert(desc.numBuckets == -1)
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.locationUri == Some("/path/to/mercury"))
    assert(desc.storage.inputFormat == Some("winput"))
    assert(desc.storage.outputFormat == Some("wowput"))
    assert(desc.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc.storage.serdeProperties == Map("k1" -> "v1"))
    assert(desc.properties == Map("k1" -> "v1", "k2" -> "v2"))
    assert(desc.comment == Some("no comment"))
  }

  test("create view -- basic") {
    val v1 = "CREATE VIEW view1 AS SELECT * FROM tab1"
    val (desc, exists) = extractTableDesc(v1)
    assert(!exists)
    assert(desc.identifier.database.isEmpty)
    assert(desc.identifier.table == "view1")
    assert(desc.tableType == CatalogTableType.VIEW)
    assert(desc.storage.locationUri.isEmpty)
    assert(desc.schema == Seq.empty[CatalogColumn])
    assert(desc.viewText == Option("SELECT * FROM tab1"))
    assert(desc.viewOriginalText == Option("SELECT * FROM tab1"))
    assert(desc.storage.serdeProperties == Map())
    assert(desc.storage.inputFormat.isEmpty)
    assert(desc.storage.outputFormat.isEmpty)
    assert(desc.storage.serde.isEmpty)
    assert(desc.properties == Map())
  }

  test("create view - full") {
    val v1 =
      """
        |CREATE OR REPLACE VIEW view1
        |(col1, col3)
        |COMMENT 'BLABLA'
        |TBLPROPERTIES('prop1Key'="prop1Val")
        |AS SELECT * FROM tab1
      """.stripMargin
    val (desc, exists) = extractTableDesc(v1)
    assert(desc.identifier.database.isEmpty)
    assert(desc.identifier.table == "view1")
    assert(desc.tableType == CatalogTableType.VIEW)
    assert(desc.storage.locationUri.isEmpty)
    assert(desc.schema ==
      CatalogColumn("col1", null, nullable = true, None) ::
        CatalogColumn("col3", null, nullable = true, None) :: Nil)
    assert(desc.viewText == Option("SELECT * FROM tab1"))
    assert(desc.viewOriginalText == Option("SELECT * FROM tab1"))
    assert(desc.storage.serdeProperties == Map())
    assert(desc.storage.inputFormat.isEmpty)
    assert(desc.storage.outputFormat.isEmpty)
    assert(desc.storage.serde.isEmpty)
    assert(desc.properties == Map("prop1Key" -> "prop1Val"))
    assert(desc.comment == Option("BLABLA"))
  }

  test("create view -- partitioned view") {
    val v1 = "CREATE VIEW view1 partitioned on (ds, hr) as select * from srcpart"
    intercept[ParseException] {
      parser.parsePlan(v1)
    }
  }

  test("MSCK repair table (not supported)") {
    assertUnsupported("MSCK REPAIR TABLE tab1")
  }

  test("create table like") {
    val v1 = "CREATE TABLE table1 LIKE table2"
    val (target, source, exists) = parser.parsePlan(v1).collect {
      case CreateTableLike(t, s, allowExisting) => (t, s, allowExisting)
    }.head
    assert(exists == false)
    assert(target.database.isEmpty)
    assert(target.table == "table1")
    assert(source.database.isEmpty)
    assert(source.table == "table2")

    val v2 = "CREATE TABLE IF NOT EXISTS table1 LIKE table2"
    val (target2, source2, exists2) = parser.parsePlan(v2).collect {
      case CreateTableLike(t, s, allowExisting) => (t, s, allowExisting)
    }.head
    assert(exists2)
    assert(target2.database.isEmpty)
    assert(target2.table == "table1")
    assert(source2.database.isEmpty)
    assert(source2.table == "table2")
  }

  test("load data") {
    val v1 = "LOAD DATA INPATH 'path' INTO TABLE table1"
    val (table, path, isLocal, isOverwrite, partition) = parser.parsePlan(v1).collect {
      case LoadData(t, path, l, o, partition) => (t, path, l, o, partition)
    }.head
    assert(table.database.isEmpty)
    assert(table.table == "table1")
    assert(path == "path")
    assert(!isLocal)
    assert(!isOverwrite)
    assert(partition.isEmpty)

    val v2 = "LOAD DATA LOCAL INPATH 'path' OVERWRITE INTO TABLE table1 PARTITION(c='1', d='2')"
    val (table2, path2, isLocal2, isOverwrite2, partition2) = parser.parsePlan(v2).collect {
      case LoadData(t, path, l, o, partition) => (t, path, l, o, partition)
    }.head
    assert(table2.database.isEmpty)
    assert(table2.table == "table1")
    assert(path2 == "path")
    assert(isLocal2)
    assert(isOverwrite2)
    assert(partition2.nonEmpty)
    assert(partition2.get.apply("c") == "1" && partition2.get.apply("d") == "2")
  }

}
