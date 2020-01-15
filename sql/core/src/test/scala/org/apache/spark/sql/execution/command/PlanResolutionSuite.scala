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

import java.net.URI
import java.util.Locale

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, CTESubstitution, EmptyFunctionRegistry, NoSuchTableException, ResolveCatalogs, ResolveSessionCatalog, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar, UnresolvedSubqueryColumnAliases, UnresolvedV2Relation}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, InSubquery, IntegerLiteral, ListQuery, StringLiteral}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, Assignment, CreateTableAsSelect, CreateV2Table, DeleteAction, DeleteFromTable, DescribeTable, DropTable, InsertAction, LogicalPlan, MergeIntoTable, OneRowRelation, Project, SubqueryAlias, UpdateAction, UpdateTable}
import org.apache.spark.sql.connector.InMemoryTableProvider
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, Identifier, Table, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CharType, DoubleType, HIVE_TYPE_STRING, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType}

class PlanResolutionSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val v2Format = classOf[InMemoryTableProvider].getName

  private val table: Table = {
    val t = mock(classOf[Table])
    when(t.schema()).thenReturn(new StructType().add("i", "int"))
    t
  }

  private val table1: Table = {
    val t = mock(classOf[Table])
    when(t.schema()).thenReturn(new StructType().add("i", "int"))
    t
  }

  private val testCat: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "tab" =>
          table
        case "tab1" =>
          table1
        case name =>
          throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn("testcat")
    newCatalog
  }

  private val v2SessionCatalog: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "v1Table" =>
          val v1Table = mock(classOf[V1Table])
          when(v1Table.schema).thenReturn(new StructType().add("i", "int"))
          v1Table
        case "v1Table1" =>
          val v1Table1 = mock(classOf[V1Table])
          when(v1Table1.schema).thenReturn(new StructType().add("i", "int"))
          v1Table1
        case "v2Table" =>
          table
        case "v2Table1" =>
          table1
        case name =>
          throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn(CatalogManager.SESSION_CATALOG_NAME)
    newCatalog
  }

  private val v1SessionCatalog: SessionCatalog = new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))

  private val catalogManagerWithDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case "testcat" =>
          testCat
        case name =>
          throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(manager.currentCatalog).thenReturn(testCat)
    when(manager.currentNamespace).thenReturn(Array.empty[String])
    when(manager.v1SessionCatalog).thenReturn(v1SessionCatalog)
    manager
  }

  private val catalogManagerWithoutDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case "testcat" =>
          testCat
        case name =>
          throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(manager.currentCatalog).thenReturn(v2SessionCatalog)
    when(manager.v1SessionCatalog).thenReturn(v1SessionCatalog)
    manager
  }

  def parseAndResolve(query: String, withDefault: Boolean = false): LogicalPlan = {
    val catalogManager = if (withDefault) {
      catalogManagerWithDefault
    } else {
      catalogManagerWithoutDefault
    }
    val analyzer = new Analyzer(catalogManager, conf)
    val rules = Seq(
      CTESubstitution,
      new ResolveCatalogs(catalogManager),
      new ResolveSessionCatalog(catalogManager, conf, _ == Seq("v")),
      analyzer.ResolveTables,
      analyzer.ResolveRelations)
    rules.foldLeft(parsePlan(query)) {
      case (plan, rule) => rule.apply(plan)
    }
  }

  private def parseResolveCompare(query: String, expected: LogicalPlan): Unit =
    comparePlans(parseAndResolve(query), expected, checkAnalysis = true)

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parseAndResolve(sql).collect {
      case CreateTable(tableDesc, mode, _) => (tableDesc, mode == SaveMode.Ignore)
    }.head
  }

  test("create table - with partitioned by") {
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

    parseAndResolve(query) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - partitioned by transforms") {
    val transforms = Seq(
        "bucket(16, b)", "years(ts)", "months(ts)", "days(ts)", "hours(ts)", "foo(a, 'bar', 34)",
        "bucket(32, b), days(ts)")
    transforms.foreach { transform =>
      val query =
        s"""
           |CREATE TABLE my_tab(a INT, b STRING) USING parquet
           |PARTITIONED BY ($transform)
           """.stripMargin

      val ae = intercept[AnalysisException] {
        parseAndResolve(query)
      }

      assert(ae.message
          .contains(s"Transforms cannot be converted to partition columns: $transform"))
    }
  }

  test("create table - with bucket") {
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

    parseAndResolve(query) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - with comment") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"),
      comment = Some("abc"))

    parseAndResolve(sql) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with table properties") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet TBLPROPERTIES('test' = 'test')"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"),
      properties = Map("test" -> "test"))

    parseAndResolve(sql) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with location") {
    val v1 = "CREATE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab"),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(new URI("/tmp/file"))),
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"))

    parseAndResolve(v1) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $v1")
    }

    val v2 =
      """
        |CREATE TABLE my_tab(a INT, b STRING)
        |USING parquet
        |OPTIONS (path '/tmp/file')
        |LOCATION '/tmp/file'
      """.stripMargin
    val e = intercept[AnalysisException] {
      parseAndResolve(v2)
    }
    assert(e.message.contains("you can only specify one of them."))
  }

  test("create table - byte length literal table name") {
    val sql = "CREATE TABLE 1m.2g(a INT) USING parquet"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("2g", Some("1m")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType),
      provider = Some("parquet"))

    parseAndResolve(sql) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
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

    parseAndResolve(sql) match {
      case CreateTable(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Test CTAS against data source tables") {
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
      val (desc, exists) = extractTableDesc(sql)
      assert(exists)
      assert(desc.identifier.database.contains("mydb"))
      assert(desc.identifier.table == "page_view")
      assert(desc.storage.locationUri.contains(new URI("/user/external/page_view")))
      assert(desc.schema.isEmpty) // will be populated later when the table is actually created
      assert(desc.comment.contains("This is the staging page view table"))
      assert(desc.viewText.isEmpty)
      assert(desc.viewCatalogAndNamespace.isEmpty)
      assert(desc.viewQueryColumnNames.isEmpty)
      assert(desc.partitionColumnNames.isEmpty)
      assert(desc.provider.contains("parquet"))
      assert(desc.properties == Map("p1" -> "v1", "p2" -> "v2"))
    }
  }

  test("Test v2 CreateTable with known catalog in identifier") {
    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS testcat.mydb.table_name (
         |    id bigint,
         |    description string,
         |    point struct<x: double, y: double>)
         |USING parquet
         |COMMENT 'table comment'
         |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
         |OPTIONS (path 's3://bucket/path/to/data', other 20)
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment")

    parseAndResolve(sql) match {
      case create: CreateV2Table =>
        assert(create.catalog.name == "testcat")
        assert(create.tableName == Identifier.of(Array("mydb"), "table_name"))
        assert(create.tableSchema == new StructType()
            .add("id", LongType)
            .add("description", StringType)
            .add("point", new StructType().add("x", DoubleType).add("y", DoubleType)))
        assert(create.partitioning.isEmpty)
        assert(create.properties == expectedProperties)
        assert(create.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateV2Table].getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Test v2 CreateTable with default catalog") {
    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS mydb.table_name (
         |    id bigint,
         |    description string,
         |    point struct<x: double, y: double>)
         |USING parquet
         |COMMENT 'table comment'
         |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
         |OPTIONS (path 's3://bucket/path/to/data', other 20)
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment")

    parseAndResolve(sql, withDefault = true) match {
      case create: CreateV2Table =>
        assert(create.catalog.name == "testcat")
        assert(create.tableName == Identifier.of(Array("mydb"), "table_name"))
        assert(create.tableSchema == new StructType()
            .add("id", LongType)
            .add("description", StringType)
            .add("point", new StructType().add("x", DoubleType).add("y", DoubleType)))
        assert(create.partitioning.isEmpty)
        assert(create.properties == expectedProperties)
        assert(create.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateV2Table].getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Test v2 CreateTable with data source v2 provider and no default") {
    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS mydb.page_view (
         |    id bigint,
         |    description string,
         |    point struct<x: double, y: double>)
         |USING $v2Format
         |COMMENT 'This is the staging page view table'
         |LOCATION '/user/external/page_view'
         |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "provider" -> v2Format,
      "location" -> "/user/external/page_view",
      "comment" -> "This is the staging page view table")

    parseAndResolve(sql) match {
      case create: CreateV2Table =>
        assert(create.catalog.name == CatalogManager.SESSION_CATALOG_NAME)
        assert(create.tableName == Identifier.of(Array("mydb"), "page_view"))
        assert(create.tableSchema == new StructType()
            .add("id", LongType)
            .add("description", StringType)
            .add("point", new StructType().add("x", DoubleType).add("y", DoubleType)))
        assert(create.partitioning.isEmpty)
        assert(create.properties == expectedProperties)
        assert(create.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateV2Table].getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Test v2 CTAS with known catalog in identifier") {
    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS testcat.mydb.table_name
         |USING parquet
         |COMMENT 'table comment'
         |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
         |OPTIONS (path 's3://bucket/path/to/data', other 20)
         |AS SELECT * FROM src
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment")

    parseAndResolve(sql) match {
      case ctas: CreateTableAsSelect =>
        assert(ctas.catalog.name == "testcat")
        assert(ctas.tableName == Identifier.of(Array("mydb"), "table_name"))
        assert(ctas.properties == expectedProperties)
        assert(ctas.writeOptions == Map("other" -> "20"))
        assert(ctas.partitioning.isEmpty)
        assert(ctas.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableAsSelect].getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Test v2 CTAS with default catalog") {
    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS mydb.table_name
         |USING parquet
         |COMMENT 'table comment'
         |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
         |OPTIONS (path 's3://bucket/path/to/data', other 20)
         |AS SELECT * FROM src
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment")

    parseAndResolve(sql, withDefault = true) match {
      case ctas: CreateTableAsSelect =>
        assert(ctas.catalog.name == "testcat")
        assert(ctas.tableName == Identifier.of(Array("mydb"), "table_name"))
        assert(ctas.properties == expectedProperties)
        assert(ctas.writeOptions == Map("other" -> "20"))
        assert(ctas.partitioning.isEmpty)
        assert(ctas.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableAsSelect].getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Test v2 CTAS with data source v2 provider and no default") {
    val sql =
      s"""
        |CREATE TABLE IF NOT EXISTS mydb.page_view
        |USING $v2Format
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "provider" -> v2Format,
      "location" -> "/user/external/page_view",
      "comment" -> "This is the staging page view table")

    parseAndResolve(sql) match {
      case ctas: CreateTableAsSelect =>
        assert(ctas.catalog.name == CatalogManager.SESSION_CATALOG_NAME)
        assert(ctas.tableName == Identifier.of(Array("mydb"), "page_view"))
        assert(ctas.properties == expectedProperties)
        assert(ctas.writeOptions.isEmpty)
        assert(ctas.partitioning.isEmpty)
        assert(ctas.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableAsSelect].getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("drop table") {
    val tableName1 = "db.tab"
    val tableIdent1 = TableIdentifier("tab", Option("db"))
    val tableName2 = "tab"
    val tableIdent2 = TableIdentifier("tab", None)

    parseResolveCompare(s"DROP TABLE $tableName1",
      DropTableCommand(tableIdent1, ifExists = false, isView = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName1",
      DropTableCommand(tableIdent1, ifExists = true, isView = false, purge = false))
    parseResolveCompare(s"DROP TABLE $tableName2",
      DropTableCommand(tableIdent2, ifExists = false, isView = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName2",
      DropTableCommand(tableIdent2, ifExists = true, isView = false, purge = false))
    parseResolveCompare(s"DROP TABLE $tableName2 PURGE",
      DropTableCommand(tableIdent2, ifExists = false, isView = false, purge = true))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName2 PURGE",
      DropTableCommand(tableIdent2, ifExists = true, isView = false, purge = true))
  }

  test("drop table in v2 catalog") {
    val tableName1 = "testcat.db.tab"
    val tableIdent1 = Identifier.of(Array("db"), "tab")
    val tableName2 = "testcat.tab"
    val tableIdent2 = Identifier.of(Array.empty, "tab")

    parseResolveCompare(s"DROP TABLE $tableName1",
      DropTable(testCat, tableIdent1, ifExists = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName1",
      DropTable(testCat, tableIdent1, ifExists = true))
    parseResolveCompare(s"DROP TABLE $tableName2",
      DropTable(testCat, tableIdent2, ifExists = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName2",
      DropTable(testCat, tableIdent2, ifExists = true))
  }

  test("drop view") {
    val viewName1 = "db.view"
    val viewIdent1 = TableIdentifier("view", Option("db"))
    val viewName2 = "view"
    val viewIdent2 = TableIdentifier("view")

    parseResolveCompare(s"DROP VIEW $viewName1",
      DropTableCommand(viewIdent1, ifExists = false, isView = true, purge = false))
    parseResolveCompare(s"DROP VIEW IF EXISTS $viewName1",
      DropTableCommand(viewIdent1, ifExists = true, isView = true, purge = false))
    parseResolveCompare(s"DROP VIEW $viewName2",
      DropTableCommand(viewIdent2, ifExists = false, isView = true, purge = false))
    parseResolveCompare(s"DROP VIEW IF EXISTS $viewName2",
      DropTableCommand(viewIdent2, ifExists = true, isView = true, purge = false))
  }

  test("drop view in v2 catalog") {
    intercept[AnalysisException] {
      parseAndResolve("DROP VIEW testcat.db.view")
    }.getMessage.toLowerCase(Locale.ROOT).contains(
      "view support in catalog has not been implemented")
  }

  // ALTER VIEW view_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER VIEW view_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter view: alter view properties") {
    val sql1_view = "ALTER VIEW table_name SET TBLPROPERTIES ('test' = 'test', " +
        "'comment' = 'new_comment')"
    val sql2_view = "ALTER VIEW table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_view = "ALTER VIEW table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

    val parsed1_view = parseAndResolve(sql1_view)
    val parsed2_view = parseAndResolve(sql2_view)
    val parsed3_view = parseAndResolve(sql3_view)

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_view = AlterTableSetPropertiesCommand(
      tableIdent, Map("test" -> "test", "comment" -> "new_comment"), isView = true)
    val expected2_view = AlterTableUnsetPropertiesCommand(
      tableIdent, Seq("comment", "test"), ifExists = false, isView = true)
    val expected3_view = AlterTableUnsetPropertiesCommand(
      tableIdent, Seq("comment", "test"), ifExists = true, isView = true)

    comparePlans(parsed1_view, expected1_view)
    comparePlans(parsed2_view, expected2_view)
    comparePlans(parsed3_view, expected3_view)
  }

  // ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER TABLE table_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter table: alter table properties") {
    Seq("v1Table" -> true, "v2Table" -> false, "testcat.tab" -> false).foreach {
      case (tblName, useV1Command) =>
        val sql1 = s"ALTER TABLE $tblName SET TBLPROPERTIES ('test' = 'test', " +
          "'comment' = 'new_comment')"
        val sql2 = s"ALTER TABLE $tblName UNSET TBLPROPERTIES ('comment', 'test')"
        val sql3 = s"ALTER TABLE $tblName UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

        val parsed1 = parseAndResolve(sql1)
        val parsed2 = parseAndResolve(sql2)
        val parsed3 = parseAndResolve(sql3)

        val tableIdent = TableIdentifier(tblName, None)
        if (useV1Command) {
          val expected1 = AlterTableSetPropertiesCommand(
            tableIdent, Map("test" -> "test", "comment" -> "new_comment"), isView = false)
          val expected2 = AlterTableUnsetPropertiesCommand(
            tableIdent, Seq("comment", "test"), ifExists = false, isView = false)
          val expected3 = AlterTableUnsetPropertiesCommand(
            tableIdent, Seq("comment", "test"), ifExists = true, isView = false)

          comparePlans(parsed1, expected1)
          comparePlans(parsed2, expected2)
          comparePlans(parsed3, expected3)
        } else {
          parsed1 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.setProperty("test", "test"),
                TableChange.setProperty("comment", "new_comment")))
            case _ => fail("expect AlterTable")
          }

          parsed2 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.removeProperty("comment"),
                TableChange.removeProperty("test")))
            case _ => fail("expect AlterTable")
          }

          parsed3 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.removeProperty("comment"),
                TableChange.removeProperty("test")))
            case _ => fail("expect AlterTable")
          }
        }
    }

    val sql4 = "ALTER TABLE non_exist SET TBLPROPERTIES ('test' = 'test')"
    val sql5 = "ALTER TABLE non_exist UNSET TBLPROPERTIES ('test')"
    val parsed4 = parseAndResolve(sql4)
    val parsed5 = parseAndResolve(sql5)

    // For non-existing tables, we convert it to v2 command with `UnresolvedV2Table`
    parsed4 match {
      case AlterTable(_, _, _: UnresolvedV2Relation, _) => // OK
      case _ => fail("Expect AlterTable, but got:\n" + parsed4.treeString)
    }
    parsed5 match {
      case AlterTable(_, _, _: UnresolvedV2Relation, _) => // OK
      case _ => fail("Expect AlterTable, but got:\n" + parsed5.treeString)
    }
  }

  test("support for other types in TBLPROPERTIES") {
    Seq("v1Table" -> true, "v2Table" -> false, "testcat.tab" -> false).foreach {
      case (tblName, useV1Command) =>
        val sql =
          s"""
            |ALTER TABLE $tblName
            |SET TBLPROPERTIES ('a' = 1, 'b' = 0.1, 'c' = TRUE)
          """.stripMargin
        val parsed = parseAndResolve(sql)
        if (useV1Command) {
          val expected = AlterTableSetPropertiesCommand(
            TableIdentifier(tblName),
            Map("a" -> "1", "b" -> "0.1", "c" -> "true"),
            isView = false)

          comparePlans(parsed, expected)
        } else {
          parsed match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.setProperty("a", "1"),
                TableChange.setProperty("b", "0.1"),
                TableChange.setProperty("c", "true")))
            case _ => fail("Expect AlterTable, but got:\n" + parsed.treeString)
          }
        }
    }
  }

  test("alter table: set location") {
    Seq("v1Table" -> true, "v2Table" -> false, "testcat.tab" -> false).foreach {
      case (tblName, useV1Command) =>
        val sql = s"ALTER TABLE $tblName SET LOCATION 'new location'"
        val parsed = parseAndResolve(sql)
        if (useV1Command) {
          val expected = AlterTableSetLocationCommand(
            TableIdentifier(tblName, None),
            None,
            "new location")
          comparePlans(parsed, expected)
        } else {
          parsed match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(TableChange.setProperty("location", "new location")))
            case _ => fail("Expect AlterTable, but got:\n" + parsed.treeString)
          }
        }
    }
  }

  test("DESCRIBE TABLE") {
    Seq("v1Table" -> true, "v2Table" -> false, "testcat.tab" -> false).foreach {
      case (tblName, useV1Command) =>
        val sql1 = s"DESC TABLE $tblName"
        val sql2 = s"DESC TABLE EXTENDED $tblName"
        val parsed1 = parseAndResolve(sql1)
        val parsed2 = parseAndResolve(sql2)
        if (useV1Command) {
          val expected1 = DescribeTableCommand(TableIdentifier(tblName, None), Map.empty, false)
          val expected2 = DescribeTableCommand(TableIdentifier(tblName, None), Map.empty, true)

          comparePlans(parsed1, expected1)
          comparePlans(parsed2, expected2)
        } else {
          parsed1 match {
            case DescribeTable(_: DataSourceV2Relation, isExtended) =>
              assert(!isExtended)
            case _ => fail("Expect DescribeTable, but got:\n" + parsed1.treeString)
          }

          parsed2 match {
            case DescribeTable(_: DataSourceV2Relation, isExtended) =>
              assert(isExtended)
            case _ => fail("Expect DescribeTable, but got:\n" + parsed2.treeString)
          }
        }

        val sql3 = s"DESC TABLE $tblName PARTITION(a=1)"
        if (useV1Command) {
          val parsed3 = parseAndResolve(sql3)
          val expected3 = DescribeTableCommand(
            TableIdentifier(tblName, None), Map("a" -> "1"), false)
          comparePlans(parsed3, expected3)
        } else {
          val e = intercept[AnalysisException](parseAndResolve(sql3))
          assert(e.message.contains("DESCRIBE TABLE does not support partition for v2 tables"))
        }
    }

    // use v1 command to describe views.
    val sql4 = "DESC TABLE v"
    val parsed4 = parseAndResolve(sql4)
    assert(parsed4.isInstanceOf[DescribeTableCommand])
  }

  test("DELETE FROM") {
    Seq("v2Table", "testcat.tab").foreach { tblName =>
      val sql1 = s"DELETE FROM $tblName"
      val sql2 = s"DELETE FROM $tblName where name='Robert'"
      val sql3 = s"DELETE FROM $tblName AS t where t.name='Robert'"
      val sql4 =
        s"""
           |WITH s(name) AS (SELECT 'Robert')
           |DELETE FROM $tblName AS t WHERE t.name IN (SELECT s.name FROM s)
         """.stripMargin

      val parsed1 = parseAndResolve(sql1)
      val parsed2 = parseAndResolve(sql2)
      val parsed3 = parseAndResolve(sql3)
      val parsed4 = parseAndResolve(sql4)

      parsed1 match {
        case DeleteFromTable(_: DataSourceV2Relation, None) =>
        case _ => fail("Expect DeleteFromTable, bug got:\n" + parsed1.treeString)
      }

      parsed2 match {
        case DeleteFromTable(
          _: DataSourceV2Relation,
          Some(EqualTo(name: UnresolvedAttribute, StringLiteral("Robert")))) =>
          assert(name.name == "name")
        case _ => fail("Expect DeleteFromTable, bug got:\n" + parsed2.treeString)
      }

      parsed3 match {
        case DeleteFromTable(
          SubqueryAlias(AliasIdentifier("t", None), _: DataSourceV2Relation),
          Some(EqualTo(name: UnresolvedAttribute, StringLiteral("Robert")))) =>
          assert(name.name == "t.name")
        case _ => fail("Expect DeleteFromTable, bug got:\n" + parsed3.treeString)
      }

      parsed4 match {
        case DeleteFromTable(SubqueryAlias(AliasIdentifier("t", None), _: DataSourceV2Relation),
            Some(InSubquery(values, query))) =>
          assert(values.size == 1 && values.head.isInstanceOf[UnresolvedAttribute])
          assert(values.head.asInstanceOf[UnresolvedAttribute].name == "t.name")
          query match {
            case ListQuery(Project(projects, SubqueryAlias(AliasIdentifier("s", None),
                UnresolvedSubqueryColumnAliases(outputColumnNames, Project(_, _: OneRowRelation)))),
                _, _, _) =>
              assert(projects.size == 1 && projects.head.name == "s.name")
              assert(outputColumnNames.size == 1 && outputColumnNames.head == "name")
            case o => fail("Unexpected subquery: \n" + o.treeString)
          }

        case _ => fail("Expect DeleteFromTable, bug got:\n" + parsed4.treeString)
      }
    }
  }

  test("UPDATE TABLE") {
    Seq("v2Table", "testcat.tab").foreach { tblName =>
      val sql1 = s"UPDATE $tblName SET name='Robert', age=32"
      val sql2 = s"UPDATE $tblName AS t SET name='Robert', age=32"
      val sql3 = s"UPDATE $tblName AS t SET name='Robert', age=32 WHERE p=1"
      val sql4 =
        s"""
           |WITH s(name) AS (SELECT 'Robert')
           |UPDATE $tblName AS t
           |SET t.age=32
           |WHERE t.name IN (SELECT s.name FROM s)
         """.stripMargin

      val parsed1 = parseAndResolve(sql1)
      val parsed2 = parseAndResolve(sql2)
      val parsed3 = parseAndResolve(sql3)
      val parsed4 = parseAndResolve(sql4)

      parsed1 match {
        case UpdateTable(
            _: DataSourceV2Relation,
            Seq(Assignment(name: UnresolvedAttribute, StringLiteral("Robert")),
              Assignment(age: UnresolvedAttribute, IntegerLiteral(32))),
            None) =>
          assert(name.name == "name")
          assert(age.name == "age")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed1.treeString)
      }

      parsed2 match {
        case UpdateTable(
            SubqueryAlias(AliasIdentifier("t", None), _: DataSourceV2Relation),
            Seq(Assignment(name: UnresolvedAttribute, StringLiteral("Robert")),
              Assignment(age: UnresolvedAttribute, IntegerLiteral(32))),
            None) =>
          assert(name.name == "name")
          assert(age.name == "age")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed2.treeString)
      }

      parsed3 match {
        case UpdateTable(
            SubqueryAlias(AliasIdentifier("t", None), _: DataSourceV2Relation),
            Seq(Assignment(name: UnresolvedAttribute, StringLiteral("Robert")),
              Assignment(age: UnresolvedAttribute, IntegerLiteral(32))),
            Some(EqualTo(p: UnresolvedAttribute, IntegerLiteral(1)))) =>
          assert(name.name == "name")
          assert(age.name == "age")
          assert(p.name == "p")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed3.treeString)
      }

      parsed4 match {
        case UpdateTable(SubqueryAlias(AliasIdentifier("t", None), _: DataSourceV2Relation),
          Seq(Assignment(key: UnresolvedAttribute, IntegerLiteral(32))),
          Some(InSubquery(values, query))) =>
          assert(key.name == "t.age")
          assert(values.size == 1 && values.head.isInstanceOf[UnresolvedAttribute])
          assert(values.head.asInstanceOf[UnresolvedAttribute].name == "t.name")
          query match {
            case ListQuery(Project(projects, SubqueryAlias(AliasIdentifier("s", None),
                UnresolvedSubqueryColumnAliases(outputColumnNames, Project(_, _: OneRowRelation)))),
                _, _, _) =>
              assert(projects.size == 1 && projects.head.name == "s.name")
              assert(outputColumnNames.size == 1 && outputColumnNames.head == "name")
            case o => fail("Unexpected subquery: \n" + o.treeString)
          }

        case _ => fail("Expect UpdateTable, but got:\n" + parsed4.treeString)
      }
    }

    val sql = "UPDATE non_existing SET id=1"
    val parsed = parseAndResolve(sql)
    parsed match {
      case u: UpdateTable =>
        assert(u.table.isInstanceOf[UnresolvedRelation])
      case _ => fail("Expect UpdateTable, but got:\n" + parsed.treeString)
    }
  }

  test("alter table: alter column") {
    Seq("v1Table" -> true, "v2Table" -> false, "testcat.tab" -> false).foreach {
      case (tblName, useV1Command) =>
        val sql1 = s"ALTER TABLE $tblName ALTER COLUMN i TYPE bigint"
        val sql2 = s"ALTER TABLE $tblName ALTER COLUMN i TYPE bigint COMMENT 'new comment'"
        val sql3 = s"ALTER TABLE $tblName ALTER COLUMN i COMMENT 'new comment'"

        val parsed1 = parseAndResolve(sql1)
        val parsed2 = parseAndResolve(sql2)

        val tableIdent = TableIdentifier(tblName, None)
        if (useV1Command) {
          val newColumn = StructField("i", LongType)
          val expected1 = AlterTableChangeColumnCommand(
            tableIdent, "i", newColumn)
          val expected2 = AlterTableChangeColumnCommand(
            tableIdent, "i", newColumn.withComment("new comment"))

          comparePlans(parsed1, expected1)
          comparePlans(parsed2, expected2)

          val e1 = intercept[AnalysisException] {
            parseAndResolve(sql3)
          }
          assert(e1.getMessage.contains("ALTER COLUMN with v1 tables must specify new data type"))

          val sql4 = s"ALTER TABLE $tblName ALTER COLUMN a.b.c TYPE bigint"
          val e2 = intercept[AnalysisException] {
            parseAndResolve(sql4)
          }
          assert(e2.getMessage.contains(
            "ALTER COLUMN with qualified column is only supported with v2 tables"))

          val sql5 = s"ALTER TABLE $tblName ALTER COLUMN i TYPE char(1)"
          val builder = new MetadataBuilder
          builder.putString(HIVE_TYPE_STRING, CharType(1).catalogString)
          val newColumnWithCleanedType = StructField("i", StringType, true, builder.build())
          val expected5 = AlterTableChangeColumnCommand(
            tableIdent, "i", newColumnWithCleanedType)
          val parsed5 = parseAndResolve(sql5)
          comparePlans(parsed5, expected5)
        } else {
          val parsed3 = parseAndResolve(sql3)

          parsed1 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.updateColumnType(Array("i"), LongType)))
            case _ => fail("expect AlterTable")
          }

          parsed2 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.updateColumnType(Array("i"), LongType),
                TableChange.updateColumnComment(Array("i"), "new comment")))
            case _ => fail("expect AlterTable")
          }

          parsed3 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.updateColumnComment(Array("i"), "new comment")))
            case _ => fail("expect AlterTable")
          }
        }
    }
  }

  test("MERGE INTO TABLE") {
    Seq(("v2Table", "v2Table1"), ("testcat.tab", "testcat.tab1")).foreach {
      case(target, source) =>
        // basic
        val sql1 =
          s"""
             |MERGE INTO $target AS target
             |USING $source AS source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update') THEN UPDATE SET target.s = source.s
             |WHEN NOT MATCHED AND (target.s='insert')
             |  THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin
        // star
        val sql2 =
          s"""
             |MERGE INTO $target AS target
             |USING $source AS source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update') THEN UPDATE SET *
             |WHEN NOT MATCHED AND (target.s='insert') THEN INSERT *
           """.stripMargin
        // no additional conditions
        val sql3 =
          s"""
             |MERGE INTO $target AS target
             |USING $source AS source
             |ON target.i = source.i
             |WHEN MATCHED THEN DELETE
             |WHEN MATCHED THEN UPDATE SET target.s = source.s
             |WHEN NOT MATCHED THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin
        // using subquery
        val sql4 =
          s"""
             |MERGE INTO $target AS target
             |USING (SELECT * FROM $source) AS source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update') THEN UPDATE SET target.s = source.s
             |WHEN NOT MATCHED AND (target.s='insert')
             |  THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin
        // cte
        val sql5 =
          s"""
             |WITH source(i, s) AS
             | (SELECT * FROM $source)
             |MERGE INTO $target AS target
             |USING source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update') THEN UPDATE SET target.s = source.s
             |WHEN NOT MATCHED AND (target.s='insert')
             |THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin

        val parsed1 = parseAndResolve(sql1)
        val parsed2 = parseAndResolve(sql2)
        val parsed3 = parseAndResolve(sql3)
        val parsed4 = parseAndResolve(sql4)
        val parsed5 = parseAndResolve(sql5)

        parsed1 match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", None), _: DataSourceV2Relation),
              SubqueryAlias(AliasIdentifier("source", None), _: DataSourceV2Relation),
              EqualTo(l: UnresolvedAttribute, r: UnresolvedAttribute),
              Seq(DeleteAction(Some(EqualTo(dl: UnresolvedAttribute, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: UnresolvedAttribute, StringLiteral("update"))),
                  updateAssigns)),
              Seq(InsertAction(Some(EqualTo(il: UnresolvedAttribute, StringLiteral("insert"))),
                insertAssigns))) =>
            assert(l.name == "target.i" && r.name == "source.i")
            assert(dl.name == "target.s")
            assert(ul.name == "target.s")
            assert(il.name == "target.s")
            assert(updateAssigns.size == 1)
            assert(updateAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
                updateAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.s")
            assert(updateAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
                updateAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.s")
            assert(insertAssigns.size == 2)
            assert(insertAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
                insertAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.i")
            assert(insertAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
                insertAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.i")

          case _ => fail("Expect MergeIntoTable, but got:\n" + parsed1.treeString)
        }

        parsed2 match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", None), _: DataSourceV2Relation),
              SubqueryAlias(AliasIdentifier("source", None), _: DataSourceV2Relation),
              EqualTo(l: UnresolvedAttribute, r: UnresolvedAttribute),
              Seq(DeleteAction(Some(EqualTo(dl: UnresolvedAttribute, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: UnresolvedAttribute,
                  StringLiteral("update"))), Seq())),
              Seq(InsertAction(Some(EqualTo(il: UnresolvedAttribute, StringLiteral("insert"))),
                Seq()))) =>
            assert(l.name == "target.i" && r.name == "source.i")
            assert(dl.name == "target.s")
            assert(ul.name == "target.s")
            assert(il.name == "target.s")

          case _ => fail("Expect MergeIntoTable, but got:\n" + parsed2.treeString)
        }

        parsed3 match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", None), _: DataSourceV2Relation),
              SubqueryAlias(AliasIdentifier("source", None), _: DataSourceV2Relation),
              EqualTo(l: UnresolvedAttribute, r: UnresolvedAttribute),
              Seq(DeleteAction(None), UpdateAction(None, updateAssigns)),
              Seq(InsertAction(None, insertAssigns))) =>
            assert(l.name == "target.i" && r.name == "source.i")
            assert(updateAssigns.size == 1)
            assert(updateAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
                updateAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.s")
            assert(updateAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
                updateAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.s")
            assert(insertAssigns.size == 2)
            assert(insertAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
                insertAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.i")
            assert(insertAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
                insertAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.i")

          case _ => fail("Expect MergeIntoTable, but got:\n" + parsed3.treeString)
        }

        parsed4 match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", None), _: DataSourceV2Relation),
              SubqueryAlias(AliasIdentifier("source", None), _: Project),
              EqualTo(l: UnresolvedAttribute, r: UnresolvedAttribute),
              Seq(DeleteAction(Some(EqualTo(dl: UnresolvedAttribute, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: UnresolvedAttribute, StringLiteral("update"))),
                  updateAssigns)),
              Seq(InsertAction(Some(EqualTo(il: UnresolvedAttribute, StringLiteral("insert"))),
                insertAssigns))) =>
            assert(l.name == "target.i" && r.name == "source.i")
            assert(dl.name == "target.s")
            assert(ul.name == "target.s")
            assert(il.name == "target.s")
            assert(updateAssigns.size == 1)
            assert(updateAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
                updateAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.s")
            assert(updateAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
                updateAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.s")
            assert(insertAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
                insertAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.i")
            assert(insertAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
                insertAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.i")

          case _ => fail("Expect MergeIntoTable, but got:\n" + parsed4.treeString)
        }

        parsed5 match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", None), _: DataSourceV2Relation),
              SubqueryAlias(AliasIdentifier("source", None),
                UnresolvedSubqueryColumnAliases(outputColumnNames,
                  Project(projects, _: DataSourceV2Relation))),
              EqualTo(l: UnresolvedAttribute, r: UnresolvedAttribute),
              Seq(DeleteAction(Some(EqualTo(dl: UnresolvedAttribute, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: UnresolvedAttribute, StringLiteral("update"))),
                  updateAssigns)),
              Seq(InsertAction(Some(EqualTo(il: UnresolvedAttribute, StringLiteral("insert"))),
                insertAssigns))) =>
            assert(outputColumnNames.size == 2 &&
              outputColumnNames.head == "i" &&
              outputColumnNames.last == "s")
            assert(projects.size == 1 && projects.head.isInstanceOf[UnresolvedStar])
            assert(l.name == "target.i" && r.name == "source.i")
            assert(dl.name == "target.s")
            assert(ul.name == "target.s")
            assert(il.name == "target.s")
            assert(updateAssigns.size == 1)
            assert(updateAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
              updateAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.s")
            assert(updateAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
              updateAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.s")
            assert(insertAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
              insertAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.i")
            assert(insertAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
              insertAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.i")

          case _ => fail("Expect MergeIntoTable, but got:\n" + parsed5.treeString)
        }
    }

    // no aliases
    Seq(("v2Table", "v2Table1"),
      ("testcat.tab", "testcat.tab1")).foreach { pair =>

      val target = pair._1
      val source = pair._2

      val sql =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON $target.i = $source.i
           |WHEN MATCHED AND ($target.s='delete') THEN DELETE
           |WHEN MATCHED AND ($target.s='update') THEN UPDATE SET $target.s = $source.s
           |WHEN NOT MATCHED AND ($target.s='insert')
           |THEN INSERT ($target.i, $target.s) values ($source.i, $source.s)
         """.stripMargin

      val parsed = parseAndResolve(sql)

      parsed match {
        case MergeIntoTable(
        _: DataSourceV2Relation,
        _: DataSourceV2Relation,
        EqualTo(l: UnresolvedAttribute, r: UnresolvedAttribute),
        Seq(DeleteAction(Some(EqualTo(dl: UnresolvedAttribute, StringLiteral("delete")))),
        UpdateAction(Some(EqualTo(ul: UnresolvedAttribute, StringLiteral("update"))),
          updateAssigns)),
        Seq(InsertAction(Some(EqualTo(il: UnresolvedAttribute, StringLiteral("insert"))),
          insertAssigns))) =>
          assert(l.name == s"$target.i" && r.name == s"$source.i")
          assert(dl.name == s"$target.s")
          assert(ul.name == s"$target.s")
          assert(il.name == s"$target.s")
          assert(updateAssigns.size == 1)
          assert(updateAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
              updateAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == s"$target.s")
          assert(updateAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
              updateAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == s"$source.s")
          assert(insertAssigns.size == 2)
          assert(insertAssigns.head.key.isInstanceOf[UnresolvedAttribute] &&
              insertAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == s"$target.i")
          assert(insertAssigns.head.value.isInstanceOf[UnresolvedAttribute] &&
              insertAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == s"$source.i")

        case _ => fail("Expect MergeIntoTable, but got:\n" + parsed.treeString)
      }
    }

    val sql =
      s"""
         |MERGE INTO non_exist_target
         |USING non_exist_source
         |ON target.i = source.i
         |WHEN MATCHED THEN DELETE
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
       """.stripMargin
    val parsed = parseAndResolve(sql)
    parsed match {
      case u: MergeIntoTable =>
        assert(u.targetTable.isInstanceOf[UnresolvedRelation])
        assert(u.sourceTable.isInstanceOf[UnresolvedRelation])
      case _ => fail("Expect MergeIntoTable, but got:\n" + parsed.treeString)
    }
  }

  // TODO: add tests for more commands.
}
