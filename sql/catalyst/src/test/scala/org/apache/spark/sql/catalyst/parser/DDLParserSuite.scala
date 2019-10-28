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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.expressions.{ApplyTransform, BucketTransform, DaysTransform, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, Transform, YearsTransform}
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

  test("create/replace table using - schema") {
    val createSql = "CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) USING parquet"
    val replaceSql = "REPLACE TABLE my_tab(a INT COMMENT 'test', b STRING) USING parquet"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType()
        .add("a", IntegerType, nullable = true, "test")
        .add("b", StringType)),
      Seq.empty[Transform],
      None,
      Map.empty[String, String],
      "parquet",
      Map.empty[String, String],
      None,
      None)

    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }

    intercept("CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING) USING parquet",
      "no viable alternative at input")
  }

  test("create/replace table - with IF NOT EXISTS") {
    val sql = "CREATE TABLE IF NOT EXISTS my_tab(a INT, b STRING) USING parquet"
    testCreateOrReplaceDdl(
      sql,
      TableSpec(
        Seq("my_tab"),
        Some(new StructType().add("a", IntegerType).add("b", StringType)),
        Seq.empty[Transform],
        None,
        Map.empty[String, String],
        "parquet",
        Map.empty[String, String],
        None,
        None),
      expectedIfNotExists = true)
  }

  test("create/replace table - with partitioned by") {
    val createSql = "CREATE TABLE my_tab(a INT comment 'test', b STRING) " +
        "USING parquet PARTITIONED BY (a)"
    val replaceSql = "REPLACE TABLE my_tab(a INT comment 'test', b STRING) " +
      "USING parquet PARTITIONED BY (a)"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType()
        .add("a", IntegerType, nullable = true, "test")
        .add("b", StringType)),
      Seq(IdentityTransform(FieldReference("a"))),
      None,
      Map.empty[String, String],
      "parquet",
      Map.empty[String, String],
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - partitioned by transforms") {
    val createSql =
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

    val replaceSql =
      """
        |REPLACE TABLE my_tab (a INT, b STRING, ts TIMESTAMP) USING parquet
        |PARTITIONED BY (
        |    a,
        |    bucket(16, b),
        |    years(ts),
        |    months(ts),
        |    days(ts),
        |    hours(ts),
        |    foo(a, "bar", 34))
      """.stripMargin
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType()
        .add("a", IntegerType)
        .add("b", StringType)
        .add("ts", TimestampType)),
      Seq(
        IdentityTransform(FieldReference("a")),
        BucketTransform(LiteralValue(16, IntegerType), Seq(FieldReference("b"))),
        YearsTransform(FieldReference("ts")),
        MonthsTransform(FieldReference("ts")),
        DaysTransform(FieldReference("ts")),
        HoursTransform(FieldReference("ts")),
        ApplyTransform("foo", Seq(
          FieldReference("a"),
          LiteralValue(UTF8String.fromString("bar"), StringType),
          LiteralValue(34, IntegerType)))),
      None,
      Map.empty[String, String],
      "parquet",
      Map.empty[String, String],
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with bucket") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
        "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet " +
      "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      Seq.empty[Transform],
      Some(BucketSpec(5, Seq("a"), Seq("b"))),
      Map.empty[String, String],
      "parquet",
      Map.empty[String, String],
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with comment") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"
    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      Seq.empty[Transform],
      None,
      Map.empty[String, String],
      "parquet",
      Map.empty[String, String],
      None,
      Some("abc"))
    Seq(createSql, replaceSql).foreach{ sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with table properties") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet" +
      " TBLPROPERTIES('test' = 'test')"
    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet" +
      " TBLPROPERTIES('test' = 'test')"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      Seq.empty[Transform],
      None,
      Map("test" -> "test"),
      "parquet",
      Map.empty[String, String],
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with location") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"
    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"
    val expectedTableSpec = TableSpec(
        Seq("my_tab"),
        Some(new StructType().add("a", IntegerType).add("b", StringType)),
        Seq.empty[Transform],
        None,
        Map.empty[String, String],
        "parquet",
        Map.empty[String, String],
        Some("/tmp/file"),
        None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - byte length literal table name") {
    val createSql = "CREATE TABLE 1m.2g(a INT) USING parquet"
    val replaceSql = "REPLACE TABLE 1m.2g(a INT) USING parquet"
    val expectedTableSpec = TableSpec(
      Seq("1m", "2g"),
      Some(new StructType().add("a", IntegerType)),
      Seq.empty[Transform],
      None,
      Map.empty[String, String],
      "parquet",
      Map.empty[String, String],
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("Duplicate clauses - create/replace table") {
    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) USING parquet $duplicateClause $duplicateClause"
    }

    def replaceTableHeader(duplicateClause: String): String = {
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

    intercept(replaceTableHeader("TBLPROPERTIES('test' = 'test2')"),
      "Found duplicate clauses: TBLPROPERTIES")
    intercept(replaceTableHeader("LOCATION '/tmp/file'"),
      "Found duplicate clauses: LOCATION")
    intercept(replaceTableHeader("COMMENT 'a table'"),
      "Found duplicate clauses: COMMENT")
    intercept(replaceTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS"),
      "Found duplicate clauses: CLUSTERED BY")
    intercept(replaceTableHeader("PARTITIONED BY (b)"),
      "Found duplicate clauses: PARTITIONED BY")
  }

  test("support for other types in OPTIONS") {
    val createSql =
      """
        |CREATE TABLE table_name USING json
        |OPTIONS (a 1, b 0.1, c TRUE)
      """.stripMargin
    val replaceSql =
      """
        |REPLACE TABLE table_name USING json
        |OPTIONS (a 1, b 0.1, c TRUE)
      """.stripMargin
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(
        sql,
        TableSpec(
          Seq("table_name"),
          Some(new StructType),
          Seq.empty[Transform],
          Option.empty[BucketSpec],
          Map.empty[String, String],
          "json",
          Map("a" -> "1", "b" -> "0.1", "c" -> "true"),
          None,
          None),
        expectedIfNotExists = false)
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

    val s4 =
      """
        |REPLACE TABLE mydb.page_view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val expectedTableSpec = TableSpec(
        Seq("mydb", "page_view"),
        None,
        Seq.empty[Transform],
        None,
        Map("p1" -> "v1", "p2" -> "v2"),
        "parquet",
        Map.empty[String, String],
        Some("/user/external/page_view"),
        Some("This is the staging page view table"))
    Seq(s1, s2, s3, s4).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = true)
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

  private def testCreateOrReplaceDdl(
      sqlStatement: String,
      tableSpec: TableSpec,
      expectedIfNotExists: Boolean): Unit = {
    val parsedPlan = parsePlan(sqlStatement)
    val newTableToken = sqlStatement.split(" ")(0).trim.toUpperCase(Locale.ROOT)
    parsedPlan match {
      case create: CreateTableStatement if newTableToken == "CREATE" =>
        assert(create.ifNotExists == expectedIfNotExists)
      case ctas: CreateTableAsSelectStatement if newTableToken == "CREATE" =>
        assert(ctas.ifNotExists == expectedIfNotExists)
      case replace: ReplaceTableStatement if newTableToken == "REPLACE" =>
      case replace: ReplaceTableAsSelectStatement if newTableToken == "REPLACE" =>
      case other =>
        fail("First token in statement does not match the expected parsed plan; CREATE TABLE" +
          " should create a CreateTableStatement, and REPLACE TABLE should create a" +
          s" ReplaceTableStatement. Statement: $sqlStatement, plan type:" +
          s" ${parsedPlan.getClass.getName}.")
    }
    assert(TableSpec(parsedPlan) === tableSpec)
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

  test("describe table column") {
    comparePlans(parsePlan("DESCRIBE t col"),
      DescribeColumnStatement(
        Seq("t"), Seq("col"), isExtended = false))
    comparePlans(parsePlan("DESCRIBE t `abc.xyz`"),
      DescribeColumnStatement(
        Seq("t"), Seq("abc.xyz"), isExtended = false))
    comparePlans(parsePlan("DESCRIBE t abc.xyz"),
      DescribeColumnStatement(
        Seq("t"), Seq("abc", "xyz"), isExtended = false))
    comparePlans(parsePlan("DESCRIBE t `a.b`.`x.y`"),
      DescribeColumnStatement(
        Seq("t"), Seq("a.b", "x.y"), isExtended = false))

    comparePlans(parsePlan("DESCRIBE TABLE t col"),
      DescribeColumnStatement(
        Seq("t"), Seq("col"), isExtended = false))
    comparePlans(parsePlan("DESCRIBE TABLE EXTENDED t col"),
      DescribeColumnStatement(
        Seq("t"), Seq("col"), isExtended = true))
    comparePlans(parsePlan("DESCRIBE TABLE FORMATTED t col"),
      DescribeColumnStatement(
        Seq("t"), Seq("col"), isExtended = true))

    val caught = intercept[AnalysisException](
      parsePlan("DESCRIBE TABLE t PARTITION (ds='1970-01-01') col"))
    assert(caught.getMessage.contains(
        "DESC TABLE COLUMN for a specific partition is not supported"))
  }

  test("SPARK-17328 Fix NPE with EXPLAIN DESCRIBE TABLE") {
    comparePlans(parsePlan("describe t"),
      DescribeTableStatement(Seq("t"), Map.empty, isExtended = false))
    comparePlans(parsePlan("describe table t"),
      DescribeTableStatement(Seq("t"), Map.empty, isExtended = false))
    comparePlans(parsePlan("describe table extended t"),
      DescribeTableStatement(Seq("t"), Map.empty, isExtended = true))
    comparePlans(parsePlan("describe table formatted t"),
      DescribeTableStatement(Seq("t"), Map.empty, isExtended = true))
  }

  test("insert table: basic append") {
    Seq(
      "INSERT INTO TABLE testcat.ns1.ns2.tbl SELECT * FROM source",
      "INSERT INTO testcat.ns1.ns2.tbl SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = false, ifPartitionNotExists = false))
    }
  }

  test("insert table: append from another catalog") {
    parseCompare("INSERT INTO TABLE testcat.ns1.ns2.tbl SELECT * FROM testcat2.db.tbl",
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map.empty,
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("testcat2", "db", "tbl"))),
        overwrite = false, ifPartitionNotExists = false))
  }

  test("insert table: append with partition") {
    parseCompare(
      """
        |INSERT INTO testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2)
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3"), "p2" -> None),
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = false, ifPartitionNotExists = false))
  }

  test("insert table: overwrite") {
    Seq(
      "INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl SELECT * FROM source",
      "INSERT OVERWRITE testcat.ns1.ns2.tbl SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = true, ifPartitionNotExists = false))
    }
  }

  test("insert table: overwrite with partition") {
    parseCompare(
      """
        |INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2)
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3"), "p2" -> None),
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = true, ifPartitionNotExists = false))
  }

  test("insert table: overwrite with partition if not exists") {
    parseCompare(
      """
        |INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3) IF NOT EXISTS
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3")),
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = true, ifPartitionNotExists = true))
  }

  test("insert table: if not exists with dynamic partition fails") {
    val exc = intercept[AnalysisException] {
      parsePlan(
        """
          |INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
          |PARTITION (p1 = 3, p2) IF NOT EXISTS
          |SELECT * FROM source
        """.stripMargin)
    }

    assert(exc.getMessage.contains("IF NOT EXISTS with dynamic partitions"))
    assert(exc.getMessage.contains("p2"))
  }

  test("insert table: if not exists without overwrite fails") {
    val exc = intercept[AnalysisException] {
      parsePlan(
        """
          |INSERT INTO TABLE testcat.ns1.ns2.tbl
          |PARTITION (p1 = 3) IF NOT EXISTS
          |SELECT * FROM source
        """.stripMargin)
    }

    assert(exc.getMessage.contains("INSERT INTO ... IF NOT EXISTS"))
  }

  test("delete from table: delete all") {
    parseCompare("DELETE FROM testcat.ns1.ns2.tbl",
      DeleteFromStatement(
        Seq("testcat", "ns1", "ns2", "tbl"),
        None,
        None))
  }

  test("delete from table: with alias and where clause") {
    parseCompare("DELETE FROM testcat.ns1.ns2.tbl AS t WHERE t.a = 2",
      DeleteFromStatement(
        Seq("testcat", "ns1", "ns2", "tbl"),
        Some("t"),
        Some(EqualTo(UnresolvedAttribute("t.a"), Literal(2)))))
  }

  test("delete from table: columns aliases is not allowed") {
    val exc = intercept[ParseException] {
      parsePlan("DELETE FROM testcat.ns1.ns2.tbl AS t(a,b,c,d) WHERE d = 2")
    }

    assert(exc.getMessage.contains("Columns aliases is not allowed in DELETE."))
  }

  test("update table: basic") {
    parseCompare(
      """
        |UPDATE testcat.ns1.ns2.tbl
        |SET t.a='Robert', t.b=32
      """.stripMargin,
      UpdateTableStatement(
        Seq("testcat", "ns1", "ns2", "tbl"),
        None,
        Seq(Seq("t", "a"), Seq("t", "b")),
        Seq(Literal("Robert"), Literal(32)),
        None))
  }

  test("update table: with alias and where clause") {
    parseCompare(
      """
        |UPDATE testcat.ns1.ns2.tbl AS t
        |SET t.a='Robert', t.b=32
        |WHERE t.c=2
      """.stripMargin,
      UpdateTableStatement(
        Seq("testcat", "ns1", "ns2", "tbl"),
        Some("t"),
        Seq(Seq("t", "a"), Seq("t", "b")),
        Seq(Literal("Robert"), Literal(32)),
        Some(EqualTo(UnresolvedAttribute("t.c"), Literal(2)))))
  }

  test("update table: columns aliases is not allowed") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |UPDATE testcat.ns1.ns2.tbl AS t(a,b,c,d)
          |SET b='Robert', c=32
          |WHERE d=2
        """.stripMargin)
    }

    assert(exc.getMessage.contains("Columns aliases is not allowed in UPDATE."))
  }

  test("show tables") {
    comparePlans(
      parsePlan("SHOW TABLES"),
      ShowTablesStatement(None, None))
    comparePlans(
      parsePlan("SHOW TABLES FROM testcat.ns1.ns2.tbl"),
      ShowTablesStatement(Some(Seq("testcat", "ns1", "ns2", "tbl")), None))
    comparePlans(
      parsePlan("SHOW TABLES IN testcat.ns1.ns2.tbl"),
      ShowTablesStatement(Some(Seq("testcat", "ns1", "ns2", "tbl")), None))
    comparePlans(
      parsePlan("SHOW TABLES IN tbl LIKE '*dog*'"),
      ShowTablesStatement(Some(Seq("tbl")), Some("*dog*")))
  }

  test("create namespace -- backward compatibility with DATABASE/DBPROPERTIES") {
    val expected = CreateNamespaceStatement(
      Seq("a", "b", "c"),
      ifNotExists = true,
      Map(
        "a" -> "a",
        "b" -> "b",
        "c" -> "c",
        "comment" -> "namespace_comment",
        "location" -> "/home/user/db"))

    comparePlans(
      parsePlan(
        """
          |CREATE NAMESPACE IF NOT EXISTS a.b.c
          |WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')
          |COMMENT 'namespace_comment' LOCATION '/home/user/db'
        """.stripMargin),
      expected)

    comparePlans(
      parsePlan(
        """
          |CREATE DATABASE IF NOT EXISTS a.b.c
          |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
          |COMMENT 'namespace_comment' LOCATION '/home/user/db'
        """.stripMargin),
      expected)
  }

  test("create namespace -- check duplicates") {
    def createDatabase(duplicateClause: String): String = {
      s"""
         |CREATE NAMESPACE IF NOT EXISTS a.b.c
         |$duplicateClause
         |$duplicateClause
      """.stripMargin
    }
    val sql1 = createDatabase("COMMENT 'namespace_comment'")
    val sql2 = createDatabase("LOCATION '/home/user/db'")
    val sql3 = createDatabase("WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')")
    val sql4 = createDatabase("WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")

    intercept(sql1, "Found duplicate clauses: COMMENT")
    intercept(sql2, "Found duplicate clauses: LOCATION")
    intercept(sql3, "Found duplicate clauses: WITH PROPERTIES")
    intercept(sql4, "Found duplicate clauses: WITH DBPROPERTIES")
  }

  test("create namespace - property values must be set") {
    assertUnsupported(
      sql = "CREATE NAMESPACE a.b.c WITH PROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("create namespace -- either PROPERTIES or DBPROPERTIES is allowed") {
    val sql =
      s"""
         |CREATE NAMESPACE IF NOT EXISTS a.b.c
         |WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')
         |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
      """.stripMargin
    intercept(sql, "Either PROPERTIES or DBPROPERTIES is allowed")
  }

  test("create namespace - support for other types in PROPERTIES") {
    val sql =
      """
        |CREATE NAMESPACE a.b.c
        |LOCATION '/home/user/db'
        |WITH PROPERTIES ('a'=1, 'b'=0.1, 'c'=TRUE)
      """.stripMargin
    comparePlans(
      parsePlan(sql),
      CreateNamespaceStatement(
        Seq("a", "b", "c"),
        ifNotExists = false,
        Map(
          "a" -> "1",
          "b" -> "0.1",
          "c" -> "true",
          "location" -> "/home/user/db")))
  }

  test("show databases: basic") {
    comparePlans(
      parsePlan("SHOW DATABASES"),
      ShowNamespacesStatement(None, None))
    comparePlans(
      parsePlan("SHOW DATABASES LIKE 'defau*'"),
      ShowNamespacesStatement(None, Some("defau*")))
  }

  test("show databases: FROM/IN operator is not allowed") {
    def verify(sql: String): Unit = {
      val exc = intercept[ParseException] { parsePlan(sql) }
      assert(exc.getMessage.contains("FROM/IN operator is not allowed in SHOW DATABASES"))
    }

    verify("SHOW DATABASES FROM testcat.ns1.ns2")
    verify("SHOW DATABASES IN testcat.ns1.ns2")
  }

  test("show namespaces") {
    comparePlans(
      parsePlan("SHOW NAMESPACES"),
      ShowNamespacesStatement(None, None))
    comparePlans(
      parsePlan("SHOW NAMESPACES FROM testcat.ns1.ns2"),
      ShowNamespacesStatement(Some(Seq("testcat", "ns1", "ns2")), None))
    comparePlans(
      parsePlan("SHOW NAMESPACES IN testcat.ns1.ns2"),
      ShowNamespacesStatement(Some(Seq("testcat", "ns1", "ns2")), None))
    comparePlans(
      parsePlan("SHOW NAMESPACES IN testcat.ns1 LIKE '*pattern*'"),
      ShowNamespacesStatement(Some(Seq("testcat", "ns1")), Some("*pattern*")))
  }

  test("analyze table statistics") {
    comparePlans(parsePlan("analyze table a.b.c compute statistics"),
      AnalyzeTableStatement(Seq("a", "b", "c"), Map.empty, noScan = false))
    comparePlans(parsePlan("analyze table a.b.c compute statistics noscan"),
      AnalyzeTableStatement(Seq("a", "b", "c"), Map.empty, noScan = true))
    comparePlans(parsePlan("analyze table a.b.c partition (a) compute statistics nOscAn"),
      AnalyzeTableStatement(Seq("a", "b", "c"), Map("a" -> None), noScan = true))

    // Partitions specified
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS"),
      AnalyzeTableStatement(
        Seq("a", "b", "c"), Map("ds" -> Some("2008-04-09"), "hr" -> Some("11")), noScan = false))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS noscan"),
      AnalyzeTableStatement(
        Seq("a", "b", "c"), Map("ds" -> Some("2008-04-09"), "hr" -> Some("11")), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09') COMPUTE STATISTICS noscan"),
      AnalyzeTableStatement(Seq("a", "b", "c"), Map("ds" -> Some("2008-04-09")), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr) COMPUTE STATISTICS"),
      AnalyzeTableStatement(
        Seq("a", "b", "c"), Map("ds" -> Some("2008-04-09"), "hr" -> None), noScan = false))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr) COMPUTE STATISTICS noscan"),
      AnalyzeTableStatement(
        Seq("a", "b", "c"), Map("ds" -> Some("2008-04-09"), "hr" -> None), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds, hr=11) COMPUTE STATISTICS noscan"),
      AnalyzeTableStatement(
        Seq("a", "b", "c"), Map("ds" -> None, "hr" -> Some("11")), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds, hr) COMPUTE STATISTICS"),
      AnalyzeTableStatement(Seq("a", "b", "c"), Map("ds" -> None, "hr" -> None), noScan = false))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds, hr) COMPUTE STATISTICS noscan"),
      AnalyzeTableStatement(Seq("a", "b", "c"), Map("ds" -> None, "hr" -> None), noScan = true))

    intercept("analyze table a.b.c compute statistics xxxx",
      "Expected `NOSCAN` instead of `xxxx`")
    intercept("analyze table a.b.c partition (a) compute statistics xxxx",
      "Expected `NOSCAN` instead of `xxxx`")
  }

  test("analyze table column statistics") {
    intercept("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR COLUMNS", "")

    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR COLUMNS key, value"),
      AnalyzeColumnStatement(Seq("a", "b", "c"), Option(Seq("key", "value")), allColumns = false))

    // Partition specified - should be ignored
    comparePlans(
      parsePlan(
        s"""
           |ANALYZE TABLE a.b.c PARTITION(ds='2017-06-10')
           |COMPUTE STATISTICS FOR COLUMNS key, value
         """.stripMargin),
      AnalyzeColumnStatement(Seq("a", "b", "c"), Option(Seq("key", "value")), allColumns = false))

    // Partition specified should be ignored in case of COMPUTE STATISTICS FOR ALL COLUMNS
    comparePlans(
      parsePlan(
        s"""
           |ANALYZE TABLE a.b.c PARTITION(ds='2017-06-10')
           |COMPUTE STATISTICS FOR ALL COLUMNS
         """.stripMargin),
      AnalyzeColumnStatement(Seq("a", "b", "c"), None, allColumns = true))

    intercept("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR ALL COLUMNS key, value",
      "mismatched input 'key' expecting <EOF>")
    intercept("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR ALL",
      "missing 'COLUMNS' at '<EOF>'")
  }

  test("MSCK REPAIR TABLE") {
    comparePlans(
      parsePlan("MSCK REPAIR TABLE a.b.c"),
      RepairTableStatement(Seq("a", "b", "c")))
  }

  test("SHOW CREATE table") {
    comparePlans(
      parsePlan("SHOW CREATE TABLE a.b.c"),
      ShowCreateTableStatement(Seq("a", "b", "c")))
  }

  test("CACHE TABLE") {
    comparePlans(
      parsePlan("CACHE TABLE a.b.c"),
      CacheTableStatement(Seq("a", "b", "c"), None, false, Map.empty))

    comparePlans(
      parsePlan("CACHE LAZY TABLE a.b.c"),
      CacheTableStatement(Seq("a", "b", "c"), None, true, Map.empty))

    comparePlans(
      parsePlan("CACHE LAZY TABLE a.b.c OPTIONS('storageLevel' 'DISK_ONLY')"),
      CacheTableStatement(Seq("a", "b", "c"), None, true, Map("storageLevel" -> "DISK_ONLY")))

    intercept("CACHE TABLE a.b.c AS SELECT * FROM testData",
      "It is not allowed to add catalog/namespace prefix a.b")
  }

  test("UNCACHE TABLE") {
    comparePlans(
      parsePlan("UNCACHE TABLE a.b.c"),
      UncacheTableStatement(Seq("a", "b", "c"), ifExists = false))

    comparePlans(
      parsePlan("UNCACHE TABLE IF EXISTS a.b.c"),
      UncacheTableStatement(Seq("a", "b", "c"), ifExists = true))
  }

  test("TRUNCATE table") {
    comparePlans(
      parsePlan("TRUNCATE TABLE a.b.c"),
      TruncateTableStatement(Seq("a", "b", "c"), None))

    comparePlans(
      parsePlan("TRUNCATE TABLE a.b.c PARTITION(ds='2017-06-10')"),
      TruncateTableStatement(Seq("a", "b", "c"), Some(Map("ds" -> "2017-06-10"))))
  }

  test("SHOW PARTITIONS") {
    val sql1 = "SHOW PARTITIONS t1"
    val sql2 = "SHOW PARTITIONS db1.t1"
    val sql3 = "SHOW PARTITIONS t1 PARTITION(partcol1='partvalue', partcol2='partvalue')"
    val sql4 = "SHOW PARTITIONS a.b.c"
    val sql5 = "SHOW PARTITIONS a.b.c PARTITION(ds='2017-06-10')"

    val parsed1 = parsePlan(sql1)
    val expected1 = ShowPartitionsStatement(Seq("t1"), None)
    val parsed2 = parsePlan(sql2)
    val expected2 = ShowPartitionsStatement(Seq("db1", "t1"), None)
    val parsed3 = parsePlan(sql3)
    val expected3 = ShowPartitionsStatement(Seq("t1"),
      Some(Map("partcol1" -> "partvalue", "partcol2" -> "partvalue")))
    val parsed4 = parsePlan(sql4)
    val expected4 = ShowPartitionsStatement(Seq("a", "b", "c"), None)
    val parsed5 = parsePlan(sql5)
    val expected5 = ShowPartitionsStatement(Seq("a", "b", "c"), Some(Map("ds" -> "2017-06-10")))

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
    comparePlans(parsed5, expected5)
  }

  test("REFRESH TABLE") {
    comparePlans(
      parsePlan("REFRESH TABLE a.b.c"),
      RefreshTableStatement(Seq("a", "b", "c")))
  }

  private case class TableSpec(
      name: Seq[String],
      schema: Option[StructType],
      partitioning: Seq[Transform],
      bucketSpec: Option[BucketSpec],
      properties: Map[String, String],
      provider: String,
      options: Map[String, String],
      location: Option[String],
      comment: Option[String])

  private object TableSpec {
    def apply(plan: LogicalPlan): TableSpec = {
      plan match {
        case create: CreateTableStatement =>
          TableSpec(
            create.tableName,
            Some(create.tableSchema),
            create.partitioning,
            create.bucketSpec,
            create.properties,
            create.provider,
            create.options,
            create.location,
            create.comment)
        case replace: ReplaceTableStatement =>
          TableSpec(
            replace.tableName,
            Some(replace.tableSchema),
            replace.partitioning,
            replace.bucketSpec,
            replace.properties,
            replace.provider,
            replace.options,
            replace.location,
            replace.comment)
        case ctas: CreateTableAsSelectStatement =>
          TableSpec(
            ctas.tableName,
            Some(ctas.asSelect).filter(_.resolved).map(_.schema),
            ctas.partitioning,
            ctas.bucketSpec,
            ctas.properties,
            ctas.provider,
            ctas.options,
            ctas.location,
            ctas.comment)
        case rtas: ReplaceTableAsSelectStatement =>
          TableSpec(
            rtas.tableName,
            Some(rtas.asSelect).filter(_.resolved).map(_.schema),
            rtas.partitioning,
            rtas.bucketSpec,
            rtas.properties,
            rtas.provider,
            rtas.options,
            rtas.location,
            rtas.comment)
        case other =>
          fail(s"Expected to parse Create, CTAS, Replace, or RTAS plan" +
            s" from query, got ${other.getClass.getName}.")
      }
    }
  }
}
