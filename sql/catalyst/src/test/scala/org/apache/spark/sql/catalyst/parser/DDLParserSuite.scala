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

package org.apache.spark.sql.catalyst.parser

import java.util.Locale

import org.apache.spark.sql.catalog.v2.expressions.{ApplyTransform, BucketTransform, DaysTransform, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, YearsTransform}
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.sql.{AlterTableAddColumnsStatement, AlterTableAlterColumnStatement, AlterTableDropColumnsStatement, AlterTableRenameColumnStatement, AlterTableSetLocationStatement, AlterTableSetPropertiesStatement, AlterTableUnsetPropertiesStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, CreateTableAsSelectStatement, CreateTableStatement, DropTableStatement, DropViewStatement, QualifiedColType}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class DDLParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
    containsThesePhrases.foreach { p =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
    }
  }

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parsePlan)(sqlCommand, messages: _*)

  private def parseCompare(sql: String, expected: LogicalPlan): Unit = {
    comparePlans(parsePlan(sql), expected, checkAnalysis = false)
  }

  test("create table using - schema") {
    val sql = "CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) USING parquet"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType()
            .add("a", IntegerType, nullable = true, "test")
            .add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }

    intercept("CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING) USING parquet",
      "no viable alternative at input")
  }

  test("create table - with IF NOT EXISTS") {
    val sql = "CREATE TABLE IF NOT EXISTS my_tab(a INT, b STRING) USING parquet"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with partitioned by") {
    val query = "CREATE TABLE my_tab(a INT comment 'test', b STRING) " +
        "USING parquet PARTITIONED BY (a)"

    parsePlan(query) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType()
            .add("a", IntegerType, nullable = true, "test")
            .add("b", StringType))
        assert(create.partitioning == Seq(IdentityTransform(FieldReference("a"))))
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - partitioned by transforms") {
    val sql =
      """
        |CREATE TABLE my_tab (a INT, b STRING, ts TIMESTAMP) USING parquet
        |PARTITIONED BY (
        |    a,
        |    bucket(16, b),
        |    years(ts),
        |    months(ts),
        |    days(ts),
        |    hours(ts),
        |    foo(a, "bar", 34))
      """.stripMargin

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType()
            .add("a", IntegerType)
            .add("b", StringType)
            .add("ts", TimestampType))
        assert(create.partitioning == Seq(
            IdentityTransform(FieldReference("a")),
            BucketTransform(LiteralValue(16, IntegerType), Seq(FieldReference("b"))),
            YearsTransform(FieldReference("ts")),
            MonthsTransform(FieldReference("ts")),
            DaysTransform(FieldReference("ts")),
            HoursTransform(FieldReference("ts")),
            ApplyTransform("foo", Seq(
                FieldReference("a"),
                LiteralValue(UTF8String.fromString("bar"), StringType),
                LiteralValue(34, IntegerType)))))
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with bucket") {
    val query = "CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
        "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    parsePlan(query) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.contains(BucketSpec(5, Seq("a"), Seq("b"))))
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - with comment") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.contains("abc"))
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with table properties") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet TBLPROPERTIES('test' = 'test')"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties == Map("test" -> "test"))
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with location") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.contains("/tmp/file"))
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - byte length literal table name") {
    val sql = "CREATE TABLE 1m.2g(a INT) USING parquet"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("1m", "2g"))
        assert(create.tableSchema == new StructType().add("a", IntegerType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Duplicate clauses - create table") {
    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) USING parquet $duplicateClause $duplicateClause"
    }

    intercept(createTableHeader("TBLPROPERTIES('test' = 'test2')"),
      "Found duplicate clauses: TBLPROPERTIES")
    intercept(createTableHeader("LOCATION '/tmp/file'"),
      "Found duplicate clauses: LOCATION")
    intercept(createTableHeader("COMMENT 'a table'"),
      "Found duplicate clauses: COMMENT")
    intercept(createTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS"),
      "Found duplicate clauses: CLUSTERED BY")
    intercept(createTableHeader("PARTITIONED BY (b)"),
      "Found duplicate clauses: PARTITIONED BY")
  }

  test("support for other types in OPTIONS") {
    val sql =
      """
        |CREATE TABLE table_name USING json
        |OPTIONS (a 1, b 0.1, c TRUE)
      """.stripMargin

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("table_name"))
        assert(create.tableSchema == new StructType)
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "json")
        assert(create.options == Map("a" -> "1", "b" -> "0.1", "c" -> "true"))
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Test CTAS against native tables") {
    val s1 =
      """
        |CREATE TABLE IF NOT EXISTS mydb.page_view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s2 =
      """
        |CREATE TABLE IF NOT EXISTS mydb.page_view
        |USING parquet
        |LOCATION '/user/external/page_view'
        |COMMENT 'This is the staging page view table'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s3 =
      """
        |CREATE TABLE IF NOT EXISTS mydb.page_view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    checkParsing(s1)
    checkParsing(s2)
    checkParsing(s3)

    def checkParsing(sql: String): Unit = {
      parsePlan(sql) match {
        case create: CreateTableAsSelectStatement =>
          assert(create.tableName == Seq("mydb", "page_view"))
          assert(create.partitioning.isEmpty)
          assert(create.bucketSpec.isEmpty)
          assert(create.properties == Map("p1" -> "v1", "p2" -> "v2"))
          assert(create.provider == "parquet")
          assert(create.options.isEmpty)
          assert(create.location.contains("/user/external/page_view"))
          assert(create.comment.contains("This is the staging page view table"))
          assert(create.ifNotExists)

        case other =>
          fail(s"Expected to parse ${classOf[CreateTableAsSelectStatement].getClass.getName} " +
              s"from query, got ${other.getClass.getName}: $sql")
      }
    }
  }

  test("drop table") {
    parseCompare("DROP TABLE testcat.ns1.ns2.tbl",
      DropTableStatement(Seq("testcat", "ns1", "ns2", "tbl"), ifExists = false, purge = false))
    parseCompare(s"DROP TABLE db.tab",
      DropTableStatement(Seq("db", "tab"), ifExists = false, purge = false))
    parseCompare(s"DROP TABLE IF EXISTS db.tab",
      DropTableStatement(Seq("db", "tab"), ifExists = true, purge = false))
    parseCompare(s"DROP TABLE tab",
      DropTableStatement(Seq("tab"), ifExists = false, purge = false))
    parseCompare(s"DROP TABLE IF EXISTS tab",
      DropTableStatement(Seq("tab"), ifExists = true, purge = false))
    parseCompare(s"DROP TABLE tab PURGE",
      DropTableStatement(Seq("tab"), ifExists = false, purge = true))
    parseCompare(s"DROP TABLE IF EXISTS tab PURGE",
      DropTableStatement(Seq("tab"), ifExists = true, purge = true))
  }

  test("drop view") {
    parseCompare(s"DROP VIEW testcat.db.view",
      DropViewStatement(Seq("testcat", "db", "view"), ifExists = false))
    parseCompare(s"DROP VIEW db.view", DropViewStatement(Seq("db", "view"), ifExists = false))
    parseCompare(s"DROP VIEW IF EXISTS db.view",
      DropViewStatement(Seq("db", "view"), ifExists = true))
    parseCompare(s"DROP VIEW view", DropViewStatement(Seq("view"), ifExists = false))
    parseCompare(s"DROP VIEW IF EXISTS view", DropViewStatement(Seq("view"), ifExists = true))
  }

  // ALTER VIEW view_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER VIEW view_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter view: alter view properties") {
    val sql1_view = "ALTER VIEW table_name SET TBLPROPERTIES ('test' = 'test', " +
        "'comment' = 'new_comment')"
    val sql2_view = "ALTER VIEW table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_view = "ALTER VIEW table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

    comparePlans(parsePlan(sql1_view),
      AlterViewSetPropertiesStatement(
      Seq("table_name"), Map("test" -> "test", "comment" -> "new_comment")))
    comparePlans(parsePlan(sql2_view),
      AlterViewUnsetPropertiesStatement(
      Seq("table_name"), Seq("comment", "test"), ifExists = false))
    comparePlans(parsePlan(sql3_view),
      AlterViewUnsetPropertiesStatement(
      Seq("table_name"), Seq("comment", "test"), ifExists = true))
  }

  // ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER TABLE table_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter table: alter table properties") {
    val sql1_table = "ALTER TABLE table_name SET TBLPROPERTIES ('test' = 'test', " +
        "'comment' = 'new_comment')"
    val sql2_table = "ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_table = "ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

    comparePlans(
      parsePlan(sql1_table),
      AlterTableSetPropertiesStatement(
        Seq("table_name"), Map("test" -> "test", "comment" -> "new_comment")))
    comparePlans(
      parsePlan(sql2_table),
      AlterTableUnsetPropertiesStatement(
        Seq("table_name"), Seq("comment", "test"), ifExists = false))
    comparePlans(
      parsePlan(sql3_table),
      AlterTableUnsetPropertiesStatement(
        Seq("table_name"), Seq("comment", "test"), ifExists = true))
  }

  test("alter table: add column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x"), IntegerType, None)
      )))
  }

  test("alter table: add multiple columns") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS x int, y string"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x"), IntegerType, None),
        QualifiedColType(Seq("y"), StringType, None)
      )))
  }

  test("alter table: add column with COLUMNS") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS x int"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x"), IntegerType, None)
      )))
  }

  test("alter table: add column with COLUMNS (...)") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS (x int)"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x"), IntegerType, None)
      )))
  }

  test("alter table: add column with COLUMNS (...) and COMMENT") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS (x int COMMENT 'doc')"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x"), IntegerType, Some("doc"))
      )))
  }

  test("alter table: add column with COMMENT") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int COMMENT 'doc'"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x"), IntegerType, Some("doc"))
      )))
  }

  test("alter table: add column with nested column name") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x.y.z int COMMENT 'doc'"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x", "y", "z"), IntegerType, Some("doc"))
      )))
  }

  test("alter table: add multiple columns with nested column name") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x.y.z int COMMENT 'doc', a.b string"),
      AlterTableAddColumnsStatement(Seq("table_name"), Seq(
        QualifiedColType(Seq("x", "y", "z"), IntegerType, Some("doc")),
        QualifiedColType(Seq("a", "b"), StringType, None)
      )))
  }

  test("alter table: add column at position (not supported)") {
    assertUnsupported("ALTER TABLE table_name ADD COLUMNS name bigint COMMENT 'doc' FIRST, a.b int")
    assertUnsupported("ALTER TABLE table_name ADD COLUMN name bigint COMMENT 'doc' FIRST")
    assertUnsupported("ALTER TABLE table_name ADD COLUMN name string AFTER a.b")
  }

  test("alter table: set location") {
    val sql1 = "ALTER TABLE table_name SET LOCATION 'new location'"
    val parsed1 = parsePlan(sql1)
    val expected1 = AlterTableSetLocationStatement(Seq("table_name"), "new location")
    comparePlans(parsed1, expected1)
  }

  test("alter table: rename column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name RENAME COLUMN a.b.c TO d"),
      AlterTableRenameColumnStatement(
        Seq("table_name"),
        Seq("a", "b", "c"),
        "d"))
  }

  test("alter table: update column type using ALTER") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bigint"),
      AlterTableAlterColumnStatement(
        Seq("table_name"),
        Seq("a", "b", "c"),
        Some(LongType),
        None))
  }

  test("alter table: update column type") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c TYPE bigint"),
      AlterTableAlterColumnStatement(
        Seq("table_name"),
        Seq("a", "b", "c"),
        Some(LongType),
        None))
  }

  test("alter table: update column comment") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c COMMENT 'new comment'"),
      AlterTableAlterColumnStatement(
        Seq("table_name"),
        Seq("a", "b", "c"),
        None,
        Some("new comment")))
  }

  test("alter table: update column type and comment") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c TYPE bigint COMMENT 'new comment'"),
      AlterTableAlterColumnStatement(
        Seq("table_name"),
        Seq("a", "b", "c"),
        Some(LongType),
        Some("new comment")))
  }

  test("alter table: change column position (not supported)") {
    assertUnsupported("ALTER TABLE table_name CHANGE COLUMN name COMMENT 'doc' FIRST")
    assertUnsupported("ALTER TABLE table_name CHANGE COLUMN name TYPE INT AFTER other_col")
  }

  test("alter table: drop column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name DROP COLUMN a.b.c"),
      AlterTableDropColumnsStatement(Seq("table_name"), Seq(Seq("a", "b", "c"))))
  }

  test("alter table: drop multiple columns") {
    val sql = "ALTER TABLE table_name DROP COLUMN x, y, a.b.c"
    Seq(sql, sql.replace("COLUMN", "COLUMNS")).foreach { drop =>
      comparePlans(
        parsePlan(drop),
        AlterTableDropColumnsStatement(
          Seq("table_name"),
          Seq(Seq("x"), Seq("y"), Seq("a", "b", "c"))))
    }
  }
}
