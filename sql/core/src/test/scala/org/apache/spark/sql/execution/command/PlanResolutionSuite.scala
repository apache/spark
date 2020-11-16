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
import java.util.{Collections, Locale}

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, CTESubstitution, EmptyFunctionRegistry, NoSuchTableException, ResolveCatalogs, ResolvedTable, ResolveInlineTables, ResolveSessionCatalog, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar, UnresolvedSubqueryColumnAliases, UnresolvedV2Relation}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, InSubquery, IntegerLiteral, ListQuery, StringLiteral}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, Assignment, CreateTableAsSelect, CreateV2Table, DeleteAction, DeleteFromTable, DescribeRelation, DropTable, InsertAction, InsertIntoStatement, LocalRelation, LogicalPlan, MergeIntoTable, OneRowRelation, Project, ShowTableProperties, SubqueryAlias, UpdateAction, UpdateTable}
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, Identifier, Table, TableCapability, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.catalog.TableChange.{UpdateColumnComment, UpdateColumnType}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.{CharType, DoubleType, HIVE_TYPE_STRING, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType}

class PlanResolutionSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val v1Format = classOf[SimpleScanSource].getName
  private val v2Format = classOf[FakeV2Provider].getName

  private val table: Table = {
    val t = mock(classOf[Table])
    when(t.schema()).thenReturn(new StructType().add("i", "int").add("s", "string"))
    t
  }

  private val tableWithAcceptAnySchemaCapability: Table = {
    val t = mock(classOf[Table])
    when(t.schema()).thenReturn(new StructType().add("i", "int"))
    when(t.capabilities()).thenReturn(Collections.singleton(TableCapability.ACCEPT_ANY_SCHEMA))
    t
  }

  private val v1Table: V1Table = {
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.tableType).thenReturn(CatalogTableType.MANAGED)
    when(t.provider).thenReturn(Some(v1Format))
    V1Table(t)
  }

  private val v1HiveTable: V1Table = {
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.tableType).thenReturn(CatalogTableType.MANAGED)
    when(t.provider).thenReturn(Some("hive"))
    V1Table(t)
  }

  private val testCat: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "tab" => table
        case "tab1" => table
        case name => throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn("testcat")
    newCatalog
  }

  private val v2SessionCatalog: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "v1Table" => v1Table
        case "v1Table1" => v1Table
        case "v1HiveTable" => v1HiveTable
        case "v2Table" => table
        case "v2Table1" => table
        case "v2TableWithAcceptAnySchemaCapability" => tableWithAcceptAnySchemaCapability
        case name => throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn(CatalogManager.SESSION_CATALOG_NAME)
    newCatalog
  }

  private val v1SessionCatalog: SessionCatalog = new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))
  v1SessionCatalog.createTempView("v", LocalRelation(Nil), false)

  private val catalogManagerWithDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case "testcat" =>
          testCat
        case CatalogManager.SESSION_CATALOG_NAME =>
          v2SessionCatalog
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
    when(manager.currentNamespace).thenReturn(Array("default"))
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
    // TODO: run the analyzer directly.
    val rules = Seq(
      CTESubstitution,
      ResolveInlineTables(conf),
      analyzer.ResolveRelations,
      new ResolveCatalogs(catalogManager),
      new ResolveSessionCatalog(catalogManager, conf, _ == Seq("v"), _ => false),
      analyzer.ResolveTables,
      analyzer.ResolveReferences,
      analyzer.ResolveSubqueryColumnAliases,
      analyzer.ResolveReferences,
      analyzer.ResolveAlterTableChanges)
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
      identifier = TableIdentifier("my_tab", Some("default")),
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
      identifier = TableIdentifier("my_tab", Some("default")),
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
      identifier = TableIdentifier("my_tab", Some("default")),
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
      identifier = TableIdentifier("my_tab", Some("default")),
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
      identifier = TableIdentifier("my_tab", Some("default")),
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
      identifier = TableIdentifier("table_name", Some("default")),
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
        assert(ctas.writeOptions.isEmpty)
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
        assert(ctas.writeOptions.isEmpty)
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
    val tableIdent2 = TableIdentifier("tab", Some("default"))

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
    val viewIdent2 = TableIdentifier("view", Option("default"))

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

    val tableIdent = TableIdentifier("table_name", Some("default"))
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

        if (useV1Command) {
          val tableIdent = TableIdentifier(tblName, Some("default"))
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
            TableIdentifier(tblName, Some("default")),
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
            TableIdentifier(tblName, Some("default")),
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

  test("DESCRIBE relation") {
    Seq("v1Table" -> true, "v2Table" -> false, "testcat.tab" -> false).foreach {
      case (tblName, useV1Command) =>
        val sql1 = s"DESC TABLE $tblName"
        val sql2 = s"DESC TABLE EXTENDED $tblName"
        val parsed1 = parseAndResolve(sql1)
        val parsed2 = parseAndResolve(sql2)
        if (useV1Command) {
          val expected1 = DescribeTableCommand(
            TableIdentifier(tblName, Some("default")), Map.empty, false)
          val expected2 = DescribeTableCommand(
            TableIdentifier(tblName, Some("default")), Map.empty, true)

          comparePlans(parsed1, expected1)
          comparePlans(parsed2, expected2)
        } else {
          parsed1 match {
            case DescribeRelation(_: ResolvedTable, _, isExtended) =>
              assert(!isExtended)
            case _ => fail("Expect DescribeTable, but got:\n" + parsed1.treeString)
          }

          parsed2 match {
            case DescribeRelation(_: ResolvedTable, _, isExtended) =>
              assert(isExtended)
            case _ => fail("Expect DescribeTable, but got:\n" + parsed2.treeString)
          }
        }

        val sql3 = s"DESC TABLE $tblName PARTITION(a=1)"
        val parsed3 = parseAndResolve(sql3)
        if (useV1Command) {
          val expected3 = DescribeTableCommand(
            TableIdentifier(tblName, Some("default")), Map("a" -> "1"), false)
          comparePlans(parsed3, expected3)
        } else {
          parsed3 match {
            case DescribeRelation(_: ResolvedTable, partitionSpec, isExtended) =>
              assert(!isExtended)
              assert(partitionSpec == Map("a" -> "1"))
            case _ => fail("Expect DescribeTable, but got:\n" + parsed2.treeString)
          }
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
        case DeleteFromTable(AsDataSourceV2Relation(_), None) =>
        case _ => fail("Expect DeleteFromTable, but got:\n" + parsed1.treeString)
      }

      parsed2 match {
        case DeleteFromTable(
          AsDataSourceV2Relation(_),
          Some(EqualTo(name: UnresolvedAttribute, StringLiteral("Robert")))) =>
          assert(name.name == "name")
        case _ => fail("Expect DeleteFromTable, but got:\n" + parsed2.treeString)
      }

      parsed3 match {
        case DeleteFromTable(
          SubqueryAlias(AliasIdentifier("t", Seq()), AsDataSourceV2Relation(_)),
          Some(EqualTo(name: UnresolvedAttribute, StringLiteral("Robert")))) =>
          assert(name.name == "t.name")
        case _ => fail("Expect DeleteFromTable, but got:\n" + parsed3.treeString)
      }

      parsed4 match {
        case DeleteFromTable(
            SubqueryAlias(AliasIdentifier("t", Seq()), AsDataSourceV2Relation(_)),
            Some(InSubquery(values, query))) =>
          assert(values.size == 1 && values.head.isInstanceOf[UnresolvedAttribute])
          assert(values.head.asInstanceOf[UnresolvedAttribute].name == "t.name")
          query match {
            case ListQuery(Project(projects, SubqueryAlias(AliasIdentifier("s", Seq()),
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
            AsDataSourceV2Relation(_),
            Seq(Assignment(name: UnresolvedAttribute, StringLiteral("Robert")),
              Assignment(age: UnresolvedAttribute, IntegerLiteral(32))),
            None) =>
          assert(name.name == "name")
          assert(age.name == "age")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed1.treeString)
      }

      parsed2 match {
        case UpdateTable(
            SubqueryAlias(AliasIdentifier("t", Seq()), AsDataSourceV2Relation(_)),
            Seq(Assignment(name: UnresolvedAttribute, StringLiteral("Robert")),
              Assignment(age: UnresolvedAttribute, IntegerLiteral(32))),
            None) =>
          assert(name.name == "name")
          assert(age.name == "age")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed2.treeString)
      }

      parsed3 match {
        case UpdateTable(
            SubqueryAlias(AliasIdentifier("t", Seq()), AsDataSourceV2Relation(_)),
            Seq(Assignment(name: UnresolvedAttribute, StringLiteral("Robert")),
              Assignment(age: UnresolvedAttribute, IntegerLiteral(32))),
            Some(EqualTo(p: UnresolvedAttribute, IntegerLiteral(1)))) =>
          assert(name.name == "name")
          assert(age.name == "age")
          assert(p.name == "p")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed3.treeString)
      }

      parsed4 match {
        case UpdateTable(SubqueryAlias(AliasIdentifier("t", Seq()), AsDataSourceV2Relation(_)),
          Seq(Assignment(key: UnresolvedAttribute, IntegerLiteral(32))),
          Some(InSubquery(values, query))) =>
          assert(key.name == "t.age")
          assert(values.size == 1 && values.head.isInstanceOf[UnresolvedAttribute])
          assert(values.head.asInstanceOf[UnresolvedAttribute].name == "t.name")
          query match {
            case ListQuery(Project(projects, SubqueryAlias(AliasIdentifier("s", Seq()),
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
        val sql2 = s"ALTER TABLE $tblName ALTER COLUMN i COMMENT 'new comment'"

        val parsed1 = parseAndResolve(sql1)
        val parsed2 = parseAndResolve(sql2)

        if (useV1Command) {
          val tableIdent = TableIdentifier(tblName, Some("default"))
          val oldColumn = StructField("i", IntegerType)
          val newColumn = StructField("i", LongType)
          val expected1 = AlterTableChangeColumnCommand(
            tableIdent, "i", newColumn)
          val expected2 = AlterTableChangeColumnCommand(
            tableIdent, "i", oldColumn.withComment("new comment"))

          comparePlans(parsed1, expected1)
          comparePlans(parsed2, expected2)

          val sql3 = s"ALTER TABLE $tblName ALTER COLUMN j COMMENT 'new comment'"
          val e1 = intercept[AnalysisException] {
            parseAndResolve(sql3)
          }
          assert(e1.getMessage.contains(
            "ALTER COLUMN cannot find column j in v1 table. Available: i, s"))

          val sql4 = s"ALTER TABLE $tblName ALTER COLUMN a.b.c TYPE bigint"
          val e2 = intercept[AnalysisException] {
            parseAndResolve(sql4)
          }
          assert(e2.getMessage.contains(
            "ALTER COLUMN with qualified column is only supported with v2 tables"))
        } else {
          parsed1 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.updateColumnType(Array("i"), LongType)))
            case _ => fail("expect AlterTable")
          }

          parsed2 match {
            case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
              assert(changes == Seq(
                TableChange.updateColumnComment(Array("i"), "new comment")))
            case _ => fail("expect AlterTable")
          }
        }
    }

    val sql = s"ALTER TABLE v1HiveTable ALTER COLUMN i TYPE char(1)"
    val builder = new MetadataBuilder
    builder.putString(HIVE_TYPE_STRING, CharType(1).catalogString)
    val newColumnWithCleanedType = StructField("i", StringType, true, builder.build())
    val expected = AlterTableChangeColumnCommand(
      TableIdentifier("v1HiveTable", Some("default")), "i", newColumnWithCleanedType)
    val parsed = parseAndResolve(sql)
    comparePlans(parsed, expected)
  }

  test("alter table: alter column action is not specified") {
    val e = intercept[AnalysisException] {
      parseAndResolve("ALTER TABLE v1Table ALTER COLUMN i")
    }
    assert(e.getMessage.contains(
      "ALTER TABLE table ALTER COLUMN requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER"))
  }

  test("alter table: alter column case sensitivity for v1 table") {
    val tblName = "v1Table"
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val sql = s"ALTER TABLE $tblName ALTER COLUMN I COMMENT 'new comment'"
        if (caseSensitive) {
          val e = intercept[AnalysisException] {
            parseAndResolve(sql)
          }
          assert(e.getMessage.contains(
            "ALTER COLUMN cannot find column I in v1 table. Available: i, s"))
        } else {
          val actual = parseAndResolve(sql)
          val expected = AlterTableChangeColumnCommand(
            TableIdentifier(tblName, Some("default")),
            "I",
            StructField("I", IntegerType).withComment("new comment"))
          comparePlans(actual, expected)
        }
      }
    }
  }

  test("alter table: hive style change column") {
    Seq("v2Table", "testcat.tab").foreach { tblName =>
      parseAndResolve(s"ALTER TABLE $tblName CHANGE COLUMN i i int COMMENT 'an index'") match {
        case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
          assert(changes.length == 1, "Should only have a comment change")
          assert(changes.head.isInstanceOf[UpdateColumnComment],
            s"Expected only a UpdateColumnComment change but got: ${changes.head}")
        case _ => fail("expect AlterTable")
      }

      parseAndResolve(s"ALTER TABLE $tblName CHANGE COLUMN i i long COMMENT 'an index'") match {
        case AlterTable(_, _, _: DataSourceV2Relation, changes) =>
          assert(changes.length == 2, "Should have a comment change and type change")
          assert(changes.exists(_.isInstanceOf[UpdateColumnComment]),
            s"Expected UpdateColumnComment change but got: ${changes}")
          assert(changes.exists(_.isInstanceOf[UpdateColumnType]),
            s"Expected UpdateColumnType change but got: ${changes}")
        case _ => fail("expect AlterTable")
      }
    }
  }

  val DSV2ResolutionTests = {
    val v2SessionCatalogTable = s"${CatalogManager.SESSION_CATALOG_NAME}.default.v2Table"
    Seq(
      ("ALTER TABLE testcat.tab ALTER COLUMN i TYPE bigint", false),
      ("ALTER TABLE tab ALTER COLUMN i TYPE bigint", false),
      (s"ALTER TABLE $v2SessionCatalogTable ALTER COLUMN i TYPE bigint", true),
      ("INSERT INTO TABLE tab VALUES (1)", false),
      ("INSERT INTO TABLE testcat.tab VALUES (1)", false),
      (s"INSERT INTO TABLE $v2SessionCatalogTable VALUES (1)", true),
      ("DESC TABLE tab", false),
      ("DESC TABLE testcat.tab", false),
      (s"DESC TABLE $v2SessionCatalogTable", true),
      ("SHOW TBLPROPERTIES tab", false),
      ("SHOW TBLPROPERTIES testcat.tab", false),
      (s"SHOW TBLPROPERTIES $v2SessionCatalogTable", true),
      ("SELECT * from tab", false),
      ("SELECT * from testcat.tab", false),
      (s"SELECT * from $v2SessionCatalogTable", true)
    )
  }

  DSV2ResolutionTests.foreach { case (sql, isSessionCatlog) =>
    test(s"Data source V2 relation resolution '$sql'") {
      val parsed = parseAndResolve(sql, withDefault = true)
      val catlogIdent = if (isSessionCatlog) v2SessionCatalog else testCat
      val tableIdent = if (isSessionCatlog) "v2Table" else "tab"
      parsed match {
        case AlterTable(_, _, r: DataSourceV2Relation, _) =>
          assert(r.catalog.exists(_ == catlogIdent))
          assert(r.identifier.exists(_.name() == tableIdent))
        case Project(_, AsDataSourceV2Relation(r)) =>
          assert(r.catalog.exists(_ == catlogIdent))
          assert(r.identifier.exists(_.name() == tableIdent))
        case InsertIntoStatement(r: DataSourceV2Relation, _, _, _, _) =>
          assert(r.catalog.exists(_ == catlogIdent))
          assert(r.identifier.exists(_.name() == tableIdent))
        case DescribeRelation(r: ResolvedTable, _, _) =>
          assert(r.catalog == catlogIdent)
          assert(r.identifier.name() == tableIdent)
        case ShowTableProperties(r: ResolvedTable, _) =>
          assert(r.catalog == catlogIdent)
          assert(r.identifier.name() == tableIdent)
        case ShowTablePropertiesCommand(t: TableIdentifier, _) =>
          assert(t.identifier == tableIdent)
      }
    }
  }

  test("MERGE INTO TABLE") {
    def checkResolution(
        target: LogicalPlan,
        source: LogicalPlan,
        mergeCondition: Expression,
        deleteCondAttr: Option[AttributeReference],
        updateCondAttr: Option[AttributeReference],
        insertCondAttr: Option[AttributeReference],
        updateAssigns: Seq[Assignment],
        insertAssigns: Seq[Assignment],
        starInUpdate: Boolean = false): Unit = {
      val ti = target.output.find(_.name == "i").get.asInstanceOf[AttributeReference]
      val ts = target.output.find(_.name == "s").get.asInstanceOf[AttributeReference]
      val si = source.output.find(_.name == "i").get.asInstanceOf[AttributeReference]
      val ss = source.output.find(_.name == "s").get.asInstanceOf[AttributeReference]

      mergeCondition match {
        case EqualTo(l: AttributeReference, r: AttributeReference) =>
          assert(l.sameRef(ti) && r.sameRef(si))
        case other => fail("unexpected merge condition " + other)
      }

      deleteCondAttr.foreach(a => assert(a.sameRef(ts)))
      updateCondAttr.foreach(a => assert(a.sameRef(ts)))
      insertCondAttr.foreach(a => assert(a.sameRef(ss)))

      if (starInUpdate) {
        assert(updateAssigns.size == 2)
        assert(updateAssigns(0).key.asInstanceOf[AttributeReference].sameRef(ti))
        assert(updateAssigns(0).value.asInstanceOf[AttributeReference].sameRef(si))
        assert(updateAssigns(1).key.asInstanceOf[AttributeReference].sameRef(ts))
        assert(updateAssigns(1).value.asInstanceOf[AttributeReference].sameRef(ss))
      } else {
        assert(updateAssigns.size == 1)
        assert(updateAssigns.head.key.asInstanceOf[AttributeReference].sameRef(ts))
        assert(updateAssigns.head.value.asInstanceOf[AttributeReference].sameRef(ss))
      }
      assert(insertAssigns.size == 2)
      assert(insertAssigns(0).key.asInstanceOf[AttributeReference].sameRef(ti))
      assert(insertAssigns(0).value.asInstanceOf[AttributeReference].sameRef(si))
      assert(insertAssigns(1).key.asInstanceOf[AttributeReference].sameRef(ts))
      assert(insertAssigns(1).value.asInstanceOf[AttributeReference].sameRef(ss))
    }

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
             |WHEN NOT MATCHED AND (source.s='insert')
             |  THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin
        parseAndResolve(sql1) match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(target)),
              SubqueryAlias(AliasIdentifier("source", Seq()), AsDataSourceV2Relation(source)),
              mergeCondition,
              Seq(DeleteAction(Some(EqualTo(dl: AttributeReference, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: AttributeReference, StringLiteral("update"))),
                  updateAssigns)),
              Seq(InsertAction(Some(EqualTo(il: AttributeReference, StringLiteral("insert"))),
                insertAssigns))) =>
            checkResolution(target, source, mergeCondition, Some(dl), Some(ul), Some(il),
              updateAssigns, insertAssigns)

          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }

        // star
        val sql2 =
          s"""
             |MERGE INTO $target AS target
             |USING $source AS source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update') THEN UPDATE SET *
             |WHEN NOT MATCHED AND (source.s='insert') THEN INSERT *
           """.stripMargin
        parseAndResolve(sql2) match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(target)),
              SubqueryAlias(AliasIdentifier("source", Seq()), AsDataSourceV2Relation(source)),
              mergeCondition,
              Seq(DeleteAction(Some(EqualTo(dl: AttributeReference, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: AttributeReference,
                  StringLiteral("update"))), updateAssigns)),
              Seq(InsertAction(Some(EqualTo(il: AttributeReference, StringLiteral("insert"))),
                insertAssigns))) =>
            checkResolution(target, source, mergeCondition, Some(dl), Some(ul), Some(il),
              updateAssigns, insertAssigns, starInUpdate = true)

          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }

        // no additional conditions
        val sql3 =
          s"""
             |MERGE INTO $target AS target
             |USING $source AS source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED THEN UPDATE SET target.s = source.s
             |WHEN NOT MATCHED THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin
        parseAndResolve(sql3) match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(target)),
              SubqueryAlias(AliasIdentifier("source", Seq()), AsDataSourceV2Relation(source)),
              mergeCondition,
              Seq(DeleteAction(Some(_)), UpdateAction(None, updateAssigns)),
              Seq(InsertAction(None, insertAssigns))) =>
            checkResolution(target, source, mergeCondition, None, None, None,
              updateAssigns, insertAssigns)

          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }

        // using subquery
        val sql4 =
          s"""
             |MERGE INTO $target AS target
             |USING (SELECT * FROM $source) AS source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update') THEN UPDATE SET target.s = source.s
             |WHEN NOT MATCHED AND (source.s='insert')
             |  THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin
        parseAndResolve(sql4) match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(target)),
              SubqueryAlias(AliasIdentifier("source", Seq()), source: Project),
              mergeCondition,
              Seq(DeleteAction(Some(EqualTo(dl: AttributeReference, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: AttributeReference, StringLiteral("update"))),
                  updateAssigns)),
              Seq(InsertAction(Some(EqualTo(il: AttributeReference, StringLiteral("insert"))),
                insertAssigns))) =>
            checkResolution(target, source, mergeCondition, Some(dl), Some(ul), Some(il),
              updateAssigns, insertAssigns)

          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }

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
             |WHEN NOT MATCHED AND (source.s='insert')
             |THEN INSERT (target.i, target.s) values (source.i, source.s)
           """.stripMargin
        parseAndResolve(sql5) match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(target)),
              SubqueryAlias(AliasIdentifier("source", Seq()), source: Project),
              mergeCondition,
              Seq(DeleteAction(Some(EqualTo(dl: AttributeReference, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(ul: AttributeReference, StringLiteral("update"))),
                  updateAssigns)),
              Seq(InsertAction(Some(EqualTo(il: AttributeReference, StringLiteral("insert"))),
                insertAssigns))) =>
            assert(source.output.map(_.name) == Seq("i", "s"))
            checkResolution(target, source, mergeCondition, Some(dl), Some(ul), Some(il),
              updateAssigns, insertAssigns)

          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }
    }

    // no aliases
    Seq(("v2Table", "v2Table1"),
      ("testcat.tab", "testcat.tab1")).foreach { pair =>

      val target = pair._1
      val source = pair._2

      val sql1 =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN MATCHED AND (${target}.s='delete') THEN DELETE
           |WHEN MATCHED THEN UPDATE SET s = 1
           |WHEN NOT MATCHED AND (s = 'a') THEN INSERT (i) values (i)
         """.stripMargin

      parseAndResolve(sql1) match {
        case MergeIntoTable(
            AsDataSourceV2Relation(target),
            AsDataSourceV2Relation(source),
            _,
            Seq(DeleteAction(Some(_)), UpdateAction(None, updateAssigns)),
            Seq(InsertAction(
              Some(EqualTo(il: AttributeReference, StringLiteral("a"))),
              insertAssigns))) =>
          val ti = target.output.find(_.name == "i").get
          val ts = target.output.find(_.name == "s").get
          val si = source.output.find(_.name == "i").get
          val ss = source.output.find(_.name == "s").get

          // INSERT condition is resolved with source table only, so column `s` is not ambiguous.
          assert(il.sameRef(ss))
          assert(updateAssigns.size == 1)
          // UPDATE key is resolved with target table only, so column `s` is not ambiguous.
          assert(updateAssigns.head.key.asInstanceOf[AttributeReference].sameRef(ts))
          assert(insertAssigns.size == 1)
          // INSERT key is resolved with target table only, so column `i` is not ambiguous.
          assert(insertAssigns.head.key.asInstanceOf[AttributeReference].sameRef(ti))
          // INSERT value is resolved with source table only, so column `i` is not ambiguous.
          assert(insertAssigns.head.value.asInstanceOf[AttributeReference].sameRef(si))

        case p => fail("Expect MergeIntoTable, but got:\n" + p.treeString)
      }

      val sql2 =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON i = 1
           |WHEN MATCHED THEN DELETE
         """.stripMargin
      // merge condition is resolved with both target and source tables, and we can't
      // resolve column `i` as it's ambiguous.
      val e2 = intercept[AnalysisException](parseAndResolve(sql2))
      assert(e2.message.contains("Reference 'i' is ambiguous"))

      val sql3 =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN MATCHED AND (s='delete') THEN DELETE
         """.stripMargin
      // delete condition is resolved with both target and source tables, and we can't
      // resolve column `s` as it's ambiguous.
      val e3 = intercept[AnalysisException](parseAndResolve(sql3))
      assert(e3.message.contains("Reference 's' is ambiguous"))

      val sql4 =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN MATCHED AND (s = 'a') THEN UPDATE SET i = 1
         """.stripMargin
      // update condition is resolved with both target and source tables, and we can't
      // resolve column `s` as it's ambiguous.
      val e4 = intercept[AnalysisException](parseAndResolve(sql4))
      assert(e4.message.contains("Reference 's' is ambiguous"))

      val sql5 =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN MATCHED THEN UPDATE SET s = s
         """.stripMargin
      // update value is resolved with both target and source tables, and we can't
      // resolve column `s` as it's ambiguous.
      val e5 = intercept[AnalysisException](parseAndResolve(sql5))
      assert(e5.message.contains("Reference 's' is ambiguous"))
    }

    val sql6 =
      s"""
         |MERGE INTO non_exist_target
         |USING non_exist_source
         |ON target.i = source.i
         |WHEN MATCHED AND (non_exist_target.s='delete') THEN DELETE
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
       """.stripMargin
    val parsed = parseAndResolve(sql6)
    parsed match {
      case u: MergeIntoTable =>
        assert(u.targetTable.isInstanceOf[UnresolvedRelation])
        assert(u.sourceTable.isInstanceOf[UnresolvedRelation])
      case _ => fail("Expect MergeIntoTable, but got:\n" + parsed.treeString)
    }
  }

  test("MERGE INTO TABLE - skip resolution on v2 tables that accept any schema") {
    val sql =
      s"""
         |MERGE INTO v2TableWithAcceptAnySchemaCapability AS target
         |USING v2Table AS source
         |ON target.i = source.i
         |WHEN MATCHED AND (target.s='delete') THEN DELETE
         |WHEN MATCHED AND (target.s='update') THEN UPDATE SET target.s = source.s
         |WHEN NOT MATCHED AND (target.s='insert')
         |  THEN INSERT (target.i, target.s) values (source.i, source.s)
       """.stripMargin

    parseAndResolve(sql) match {
      case MergeIntoTable(
          SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(_)),
          SubqueryAlias(AliasIdentifier("source", Seq()), AsDataSourceV2Relation(_)),
          EqualTo(l: UnresolvedAttribute, r: UnresolvedAttribute),
          Seq(
            DeleteAction(Some(EqualTo(dl: UnresolvedAttribute, StringLiteral("delete")))),
            UpdateAction(
              Some(EqualTo(ul: UnresolvedAttribute, StringLiteral("update"))),
              updateAssigns)),
          Seq(
            InsertAction(
              Some(EqualTo(il: UnresolvedAttribute, StringLiteral("insert"))),
              insertAssigns))) =>
        assert(l.name == "target.i" && r.name == "source.i")
        assert(dl.name == "target.s")
        assert(ul.name == "target.s")
        assert(il.name == "target.s")
        assert(updateAssigns.size == 1)
        assert(updateAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.s")
        assert(updateAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.s")
        assert(insertAssigns.size == 2)
        assert(insertAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.i")
        assert(insertAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "source.i")

      case l => fail("Expected unresolved MergeIntoTable, but got:\n" + l.treeString)
    }
  }

  test("SPARK-31147: forbid CHAR type in non-Hive tables") {
    def checkFailure(t: String, provider: String): Unit = {
      val types = Seq(
        "CHAR(2)",
        "ARRAY<CHAR(2)>",
        "MAP<INT, CHAR(2)>",
        "MAP<CHAR(2), INT>",
        "STRUCT<s: CHAR(2)>")
      types.foreach { tpe =>
        intercept[AnalysisException] {
          parseAndResolve(s"CREATE TABLE $t(col $tpe) USING $provider")
        }
        intercept[AnalysisException] {
          parseAndResolve(s"REPLACE TABLE $t(col $tpe) USING $provider")
        }
        intercept[AnalysisException] {
          parseAndResolve(s"CREATE OR REPLACE TABLE $t(col $tpe) USING $provider")
        }
        intercept[AnalysisException] {
          parseAndResolve(s"ALTER TABLE $t ADD COLUMN col $tpe")
        }
        intercept[AnalysisException] {
          parseAndResolve(s"ALTER TABLE $t ADD COLUMN col $tpe")
        }
        intercept[AnalysisException] {
          parseAndResolve(s"ALTER TABLE $t ALTER COLUMN col TYPE $tpe")
        }
      }
    }

    checkFailure("v1Table", v1Format)
    checkFailure("v2Table", v2Format)
    checkFailure("testcat.tab", "foo")
  }

  // TODO: add tests for more commands.
}

object AsDataSourceV2Relation {
  def unapply(plan: LogicalPlan): Option[DataSourceV2Relation] = plan match {
    case SubqueryAlias(_, r: DataSourceV2Relation) => Some(r)
    case _ => None
  }
}
