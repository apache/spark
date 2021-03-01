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
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, EmptyFunctionRegistry, NoSuchTableException, ResolvedTable, ResolveSessionCatalog, UnresolvedAttribute, UnresolvedRelation, UnresolvedSubqueryColumnAliases, UnresolvedV2Relation}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, InSubquery, IntegerLiteral, ListQuery, StringLiteral}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, AppendData, Assignment, CreateTableAsSelect, CreateTableStatement, CreateV2Table, DeleteAction, DeleteFromTable, DescribeRelation, DropTable, InsertAction, LocalRelation, LogicalPlan, MergeIntoTable, OneRowRelation, Project, ShowTableProperties, SubqueryAlias, UpdateAction, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, Identifier, Table, TableCapability, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.catalog.TableChange.{UpdateColumnComment, UpdateColumnType}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.{CharType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

class PlanResolutionSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val v1Format = classOf[SimpleScanSource].getName
  private val v2Format = classOf[FakeV2Provider].getName

  private val table: Table = {
    val t = mock(classOf[Table])
    when(t.schema()).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
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
    val analyzer = new Analyzer(catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(
        new ResolveSessionCatalog(catalogManager, _ == Seq("v"), _ => false))
    }
    // We don't check analysis here, as we expect the plan to be unresolved such as `CreateTable`.
    analyzer.execute(CatalystSqlParser.parsePlan(query))
  }

  private def parseResolveCompare(query: String, expected: LogicalPlan): Unit =
    comparePlans(parseAndResolve(query), expected, checkAnalysis = true)

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parseAndResolve(sql).collect {
      case CreateTable(tableDesc, mode, _) => (tableDesc, mode == SaveMode.Ignore)
    }.head
  }

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
    containsThesePhrases.foreach { p =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
    }
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
      "option.other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment",
      "other" -> "20")

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
      "option.other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment",
      "other" -> "20")

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
      "option.other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment",
      "other" -> "20")

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
      "option.other" -> "20",
      "provider" -> "parquet",
      "location" -> "s3://bucket/path/to/data",
      "comment" -> "table comment",
      "other" -> "20")

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
    val tableName1 = "db.v1Table"
    val tableIdent1 = TableIdentifier("v1Table", Option("db"))
    val tableName2 = "v1Table"
    val tableIdent2 = TableIdentifier("v1Table", Some("default"))

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
      DropTable(ResolvedTable(testCat, tableIdent1, table), ifExists = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName1",
      DropTable(ResolvedTable(testCat, tableIdent1, table), ifExists = true, purge = false))
    parseResolveCompare(s"DROP TABLE $tableName2",
      DropTable(ResolvedTable(testCat, tableIdent2, table), ifExists = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName2",
      DropTable(ResolvedTable(testCat, tableIdent2, table), ifExists = true, purge = false))
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
            case DescribeRelation(_: ResolvedTable, _, isExtended, _) =>
              assert(!isExtended)
            case _ => fail("Expect DescribeTable, but got:\n" + parsed1.treeString)
          }

          parsed2 match {
            case DescribeRelation(_: ResolvedTable, _, isExtended, _) =>
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
            case DescribeRelation(_: ResolvedTable, partitionSpec, isExtended, _) =>
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
    val newColumnWithCleanedType = StructField("i", CharType(1), true)
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
      ("INSERT INTO TABLE tab VALUES (1, 'a')", false),
      ("INSERT INTO TABLE testcat.tab VALUES (1, 'a')", false),
      (s"INSERT INTO TABLE $v2SessionCatalogTable VALUES (1, 'a')", true),
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

  DSV2ResolutionTests.foreach { case (sql, isSessionCatalog) =>
    test(s"Data source V2 relation resolution '$sql'") {
      val parsed = parseAndResolve(sql, withDefault = true)
      val catalogIdent = if (isSessionCatalog) v2SessionCatalog else testCat
      val tableIdent = if (isSessionCatalog) "v2Table" else "tab"
      parsed match {
        case AlterTable(_, _, r: DataSourceV2Relation, _) =>
          assert(r.catalog.exists(_ == catalogIdent))
          assert(r.identifier.exists(_.name() == tableIdent))
        case Project(_, AsDataSourceV2Relation(r)) =>
          assert(r.catalog.exists(_ == catalogIdent))
          assert(r.identifier.exists(_.name() == tableIdent))
        case AppendData(r: DataSourceV2Relation, _, _, _) =>
          assert(r.catalog.exists(_ == catalogIdent))
          assert(r.identifier.exists(_.name() == tableIdent))
        case DescribeRelation(r: ResolvedTable, _, _, _) =>
          assert(r.catalog == catalogIdent)
          assert(r.identifier.name() == tableIdent)
        case ShowTableProperties(r: ResolvedTable, _) =>
          assert(r.catalog == catalogIdent)
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

  private def compareNormalized(plan1: LogicalPlan, plan2: LogicalPlan): Unit = {
    /**
     * Normalizes plans:
     * - CreateTable the createTime in tableDesc will replaced by -1L.
     */
    def normalizePlan(plan: LogicalPlan): LogicalPlan = {
      plan match {
        case CreateTable(tableDesc, mode, query) =>
          val newTableDesc = tableDesc.copy(createTime = -1L)
          CreateTable(newTableDesc, mode, query)
        case _ => plan // Don't transform
      }
    }
    comparePlans(normalizePlan(plan1), normalizePlan(plan2))
  }

  test("create table - schema") {
    def createTable(
        table: String,
        database: Option[String] = None,
        tableType: CatalogTableType = CatalogTableType.MANAGED,
        storage: CatalogStorageFormat = CatalogStorageFormat.empty.copy(
          inputFormat = HiveSerDe.sourceToSerDe("textfile").get.inputFormat,
          outputFormat = HiveSerDe.sourceToSerDe("textfile").get.outputFormat,
          serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")),
        schema: StructType = new StructType,
        provider: Option[String] = Some("hive"),
        partitionColumnNames: Seq[String] = Seq.empty,
        comment: Option[String] = None,
        mode: SaveMode = SaveMode.ErrorIfExists,
        query: Option[LogicalPlan] = None): CreateTable = {
      CreateTable(
        CatalogTable(
          identifier = TableIdentifier(table, database),
          tableType = tableType,
          storage = storage,
          schema = schema,
          provider = provider,
          partitionColumnNames = partitionColumnNames,
          comment = comment
        ), mode, query
      )
    }

    def compare(sql: String, plan: LogicalPlan): Unit = {
      compareNormalized(parseAndResolve(sql), plan)
    }

    compare("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) STORED AS textfile",
      createTable(
        table = "my_tab",
        database = Some("default"),
        schema = (new StructType)
            .add("a", IntegerType, nullable = true, "test")
            .add("b", StringType)
      )
    )
    compare("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) STORED AS textfile " +
        "PARTITIONED BY (c INT, d STRING COMMENT 'test2')",
      createTable(
        table = "my_tab",
        database = Some("default"),
        schema = (new StructType)
            .add("a", IntegerType, nullable = true, "test")
            .add("b", StringType)
            .add("c", IntegerType)
            .add("d", StringType, nullable = true, "test2"),
        partitionColumnNames = Seq("c", "d")
      )
    )
    compare("CREATE TABLE my_tab(id BIGINT, nested STRUCT<col1: STRING,col2: INT>) " +
        "STORED AS textfile",
      createTable(
        table = "my_tab",
        database = Some("default"),
        schema = (new StructType)
            .add("id", LongType)
            .add("nested", (new StructType)
                .add("col1", StringType)
                .add("col2", IntegerType)
            )
      )
    )
    // Partitioned by a StructType should be accepted by `SparkSqlParser` but will fail an analyze
    // rule in `AnalyzeCreateTable`.
    compare("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) STORED AS textfile " +
        "PARTITIONED BY (nested STRUCT<col1: STRING,col2: INT>)",
      createTable(
        table = "my_tab",
        database = Some("default"),
        schema = (new StructType)
            .add("a", IntegerType, nullable = true, "test")
            .add("b", StringType)
            .add("nested", (new StructType)
                .add("col1", StringType)
                .add("col2", IntegerType)
            ),
        partitionColumnNames = Seq("nested")
      )
    )

    interceptParseException(parsePlan)(
      "CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING)",
      "extraneous input ':'")
  }

  test("create hive table - table file format") {
    val allSources = Seq("parquet", "parquetfile", "orc", "orcfile", "avro", "avrofile",
      "sequencefile", "rcfile", "textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab STORED AS $s"
      parseAndResolve(query) match {
        case ct: CreateTable =>
          val hiveSerde = HiveSerDe.sourceToSerDe(s)
          assert(hiveSerde.isDefined)
          assert(ct.tableDesc.storage.serde ==
            hiveSerde.get.serde.orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
          assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
          assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
      }
    }
  }

  test("create hive table - row format and table file format") {
    val createTableStart = "CREATE TABLE my_tab ROW FORMAT"
    val fileFormat = s"STORED AS INPUTFORMAT 'inputfmt' OUTPUTFORMAT 'outputfmt'"
    val query1 = s"$createTableStart SERDE 'anything' $fileFormat"
    val query2 = s"$createTableStart DELIMITED FIELDS TERMINATED BY ' ' $fileFormat"

    // No conflicting serdes here, OK
    parseAndResolve(query1) match {
      case parsed1: CreateTable =>
        assert(parsed1.tableDesc.storage.serde == Some("anything"))
        assert(parsed1.tableDesc.storage.inputFormat == Some("inputfmt"))
        assert(parsed1.tableDesc.storage.outputFormat == Some("outputfmt"))
    }

    parseAndResolve(query2) match {
      case parsed2: CreateTable =>
        assert(parsed2.tableDesc.storage.serde ==
            Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
        assert(parsed2.tableDesc.storage.inputFormat == Some("inputfmt"))
        assert(parsed2.tableDesc.storage.outputFormat == Some("outputfmt"))
    }
  }

  test("create hive table - row format serde and generic file format") {
    val allSources = Seq("parquet", "orc", "avro", "sequencefile", "rcfile", "textfile")
    val supportedSources = Set("sequencefile", "rcfile", "textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab ROW FORMAT SERDE 'anything' STORED AS $s"
      if (supportedSources.contains(s)) {
        parseAndResolve(query) match {
          case ct: CreateTable =>
            val hiveSerde = HiveSerDe.sourceToSerDe(s)
            assert(hiveSerde.isDefined)
            assert(ct.tableDesc.storage.serde == Some("anything"))
            assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
            assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
        }
      } else {
        assertUnsupported(query, Seq("row format serde", "incompatible", s))
      }
    }
  }

  test("create hive table - row format delimited and generic file format") {
    val allSources = Seq("parquet", "orc", "avro", "sequencefile", "rcfile", "textfile")
    val supportedSources = Set("textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS $s"
      if (supportedSources.contains(s)) {
        parseAndResolve(query) match {
          case ct: CreateTable =>
            val hiveSerde = HiveSerDe.sourceToSerDe(s)
            assert(hiveSerde.isDefined)
            assert(ct.tableDesc.storage.serde == hiveSerde.get.serde
                .orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
            assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
            assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
        }
      } else {
        assertUnsupported(query, Seq("row format delimited", "only compatible with 'textfile'", s))
      }
    }
  }

  test("create hive external table - location must be specified") {
    val exc = intercept[AnalysisException] {
      parseAndResolve("CREATE EXTERNAL TABLE my_tab STORED AS parquet")
    }
    assert(exc.getMessage.contains("CREATE EXTERNAL TABLE must be accompanied by LOCATION"))

    val query = "CREATE EXTERNAL TABLE my_tab STORED AS parquet LOCATION '/something/anything'"
    parseAndResolve(query) match {
      case ct: CreateTable =>
        assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
        assert(ct.tableDesc.storage.locationUri == Some(new URI("/something/anything")))
    }
  }

  test("create hive table - property values must be set") {
    assertUnsupported(
      sql = "CREATE TABLE my_tab STORED AS parquet " +
          "TBLPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
    assertUnsupported(
      sql = "CREATE TABLE my_tab ROW FORMAT SERDE 'serde' " +
          "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("create hive table - location implies external") {
    val query = "CREATE TABLE my_tab STORED AS parquet LOCATION '/something/anything'"
    parseAndResolve(query) match {
      case ct: CreateTable =>
        assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
        assert(ct.tableDesc.storage.locationUri == Some(new URI("/something/anything")))
    }
  }

  test("Duplicate clauses - create hive table") {
    def intercept(sqlCommand: String, messages: String*): Unit =
      interceptParseException(parsePlan)(sqlCommand, messages: _*)

    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) STORED AS parquet $duplicateClause $duplicateClause"
    }

    intercept(createTableHeader("TBLPROPERTIES('test' = 'test2')"),
      "Found duplicate clauses: TBLPROPERTIES")
    intercept(createTableHeader("LOCATION '/tmp/file'"),
      "Found duplicate clauses: LOCATION")
    intercept(createTableHeader("COMMENT 'a table'"),
      "Found duplicate clauses: COMMENT")
    intercept(createTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS"),
      "Found duplicate clauses: CLUSTERED BY")
    intercept(createTableHeader("PARTITIONED BY (k int)"),
      "Found duplicate clauses: PARTITIONED BY")
    intercept(createTableHeader("STORED AS parquet"),
      "Found duplicate clauses: STORED AS/BY")
    intercept(
      createTableHeader("ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'"),
      "Found duplicate clauses: ROW FORMAT")
  }

  test("Test CTAS #1") {
    val s1 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |COMMENT 'This is the staging page view table'
        |STORED AS RCFILE
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s2 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |STORED AS RCFILE
        |COMMENT 'This is the staging page view table'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |LOCATION '/user/external/page_view'
        |AS SELECT * FROM src
      """.stripMargin

    val s3 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |LOCATION '/user/external/page_view'
        |STORED AS RCFILE
        |COMMENT 'This is the staging page view table'
        |AS SELECT * FROM src
      """.stripMargin

    checkParsing(s1)
    checkParsing(s2)
    checkParsing(s3)

    def checkParsing(sql: String): Unit = {
      val (desc, exists) = extractTableDesc(sql)
      assert(exists)
      assert(desc.identifier.database == Some("mydb"))
      assert(desc.identifier.table == "page_view")
      assert(desc.tableType == CatalogTableType.EXTERNAL)
      assert(desc.storage.locationUri == Some(new URI("/user/external/page_view")))
      assert(desc.schema.isEmpty) // will be populated later when the table is actually created
      assert(desc.comment == Some("This is the staging page view table"))
      // TODO will be SQLText
      assert(desc.viewText.isEmpty)
      assert(desc.viewCatalogAndNamespace.isEmpty)
      assert(desc.viewQueryColumnNames.isEmpty)
      assert(desc.partitionColumnNames.isEmpty)
      assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
      assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
      assert(desc.storage.serde ==
          Some("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"))
      assert(desc.properties == Map("p1" -> "v1", "p2" -> "v2"))
    }
  }

  test("Test CTAS #2") {
    val s1 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |COMMENT 'This is the staging page view table'
        |ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
        | OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s2 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
        | OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
        |COMMENT 'This is the staging page view table'
        |AS SELECT * FROM src
      """.stripMargin

    checkParsing(s1)
    checkParsing(s2)

    def checkParsing(sql: String): Unit = {
      val (desc, exists) = extractTableDesc(sql)
      assert(exists)
      assert(desc.identifier.database == Some("mydb"))
      assert(desc.identifier.table == "page_view")
      assert(desc.tableType == CatalogTableType.EXTERNAL)
      assert(desc.storage.locationUri == Some(new URI("/user/external/page_view")))
      assert(desc.schema.isEmpty) // will be populated later when the table is actually created
      // TODO will be SQLText
      assert(desc.comment == Some("This is the staging page view table"))
      assert(desc.viewText.isEmpty)
      assert(desc.viewCatalogAndNamespace.isEmpty)
      assert(desc.viewQueryColumnNames.isEmpty)
      assert(desc.partitionColumnNames.isEmpty)
      assert(desc.storage.properties == Map())
      assert(desc.storage.inputFormat == Some("parquet.hive.DeprecatedParquetInputFormat"))
      assert(desc.storage.outputFormat == Some("parquet.hive.DeprecatedParquetOutputFormat"))
      assert(desc.storage.serde == Some("parquet.hive.serde.ParquetHiveSerDe"))
      assert(desc.properties == Map("p1" -> "v1", "p2" -> "v2"))
    }
  }

  test("Test CTAS #3") {
    val s3 = """CREATE TABLE page_view STORED AS textfile AS SELECT * FROM src"""
    val (desc, exists) = extractTableDesc(s3)
    assert(exists == false)
    assert(desc.identifier.database == Some("default"))
    assert(desc.identifier.table == "page_view")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.storage.locationUri == None)
    assert(desc.schema.isEmpty)
    assert(desc.viewText == None) // TODO will be SQLText
    assert(desc.viewQueryColumnNames.isEmpty)
    assert(desc.storage.properties == Map())
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
        Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
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
    assert(desc.identifier.database == Some("default"))
    assert(desc.identifier.table == "ctas2")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.storage.locationUri == None)
    assert(desc.schema.isEmpty)
    assert(desc.viewText == None) // TODO will be SQLText
    assert(desc.viewCatalogAndNamespace.isEmpty)
    assert(desc.viewQueryColumnNames.isEmpty)
    assert(desc.storage.properties == Map(("serde_p1" -> "p1"), ("serde_p2" -> "p2")))
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
    assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
    assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"))
    assert(desc.properties == Map(("tbl_p1" -> "p11"), ("tbl_p2" -> "p22")))
  }

  test("CTAS statement with a PARTITIONED BY clause is not allowed") {
    assertUnsupported(s"CREATE TABLE ctas1 PARTITIONED BY (k int)" +
        " AS SELECT key, value FROM (SELECT 1 as key, 2 as value) tmp")
  }

  test("CTAS statement with schema") {
    assertUnsupported(s"CREATE TABLE ctas1 (age INT, name STRING) AS SELECT * FROM src")
    assertUnsupported(s"CREATE TABLE ctas1 (age INT, name STRING) AS SELECT 1, 'hello'")
  }

  test("create table - basic") {
    val query = "CREATE TABLE my_table (id int, name string)"
    val (desc, allowExisting) = extractTableDesc(query)
    assert(!allowExisting)
    assert(desc.identifier.database == Some("default"))
    assert(desc.identifier.table == "my_table")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.schema == new StructType().add("id", "int").add("name", "string"))
    assert(desc.partitionColumnNames.isEmpty)
    assert(desc.bucketSpec.isEmpty)
    assert(desc.viewText.isEmpty)
    assert(desc.viewQueryColumnNames.isEmpty)
    assert(desc.storage.locationUri.isEmpty)
    assert(desc.storage.inputFormat ==
        Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
        Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    assert(desc.storage.properties.isEmpty)
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
    val e = intercept[ParseException] { parsePlan(query) }
    assert(e.message.contains("Operation not allowed: CREATE TEMPORARY TABLE"))
  }

  test("create table - external") {
    val query = "CREATE EXTERNAL TABLE tab1 (id int, name string) LOCATION '/path/to/nowhere'"
    val (desc, _) = extractTableDesc(query)
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.storage.locationUri == Some(new URI("/path/to/nowhere")))
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
    assert(desc.schema == new StructType()
        .add("id", "int")
        .add("name", "string")
        .add("month", "int"))
    assert(desc.partitionColumnNames == Seq("month"))
  }

  test("create table - clustered by") {
    val numBuckets = 10
    val bucketedColumn = "id"
    val sortColumn = "id"
    val baseQuery =
      s"""
         CREATE TABLE my_table (
           $bucketedColumn int,
           name string)
         CLUSTERED BY($bucketedColumn)
       """

    val query1 = s"$baseQuery INTO $numBuckets BUCKETS"
    val (desc1, _) = extractTableDesc(query1)
    assert(desc1.bucketSpec.isDefined)
    val bucketSpec1 = desc1.bucketSpec.get
    assert(bucketSpec1.numBuckets == numBuckets)
    assert(bucketSpec1.bucketColumnNames.head.equals(bucketedColumn))
    assert(bucketSpec1.sortColumnNames.isEmpty)

    val query2 = s"$baseQuery SORTED BY($sortColumn) INTO $numBuckets BUCKETS"
    val (desc2, _) = extractTableDesc(query2)
    assert(desc2.bucketSpec.isDefined)
    val bucketSpec2 = desc2.bucketSpec.get
    assert(bucketSpec2.numBuckets == numBuckets)
    assert(bucketSpec2.bucketColumnNames.head.equals(bucketedColumn))
    assert(bucketSpec2.sortColumnNames.head.equals(sortColumn))
  }

  test("create table(hive) - skewed by") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) SKEWED BY"
    val query1 = s"$baseQuery(id) ON (1, 10, 100)"
    val query2 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z'))"
    val query3 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z')) STORED AS DIRECTORIES"
    val e1 = intercept[ParseException] { parsePlan(query1) }
    val e2 = intercept[ParseException] { parsePlan(query2) }
    val e3 = intercept[ParseException] { parsePlan(query3) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
    assert(e3.getMessage.contains("Operation not allowed"))
  }

  test("create table(hive) - row format") {
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
    assert(desc1.storage.properties.isEmpty)
    assert(desc2.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc2.storage.properties == Map("k1" -> "v1"))
    assert(desc3.storage.properties == Map(
      "field.delim" -> "x",
      "escape.delim" -> "y",
      "serialization.format" -> "x",
      "line.delim" -> "\n",
      "colelction.delim" -> "a", // yes, it's a typo from Hive :)
      "mapkey.delim" -> "b"))
  }

  test("create table(hive) - file format") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) STORED AS"
    val query1 = s"$baseQuery INPUTFORMAT 'winput' OUTPUTFORMAT 'wowput'"
    val query2 = s"$baseQuery ORC"
    val (desc1, _) = extractTableDesc(query1)
    val (desc2, _) = extractTableDesc(query2)
    assert(desc1.storage.inputFormat == Some("winput"))
    assert(desc1.storage.outputFormat == Some("wowput"))
    assert(desc1.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    assert(desc2.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"))
    assert(desc2.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))
    assert(desc2.storage.serde == Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
  }

  test("create table(hive) - storage handler") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) STORED BY"
    val query1 = s"$baseQuery 'org.papachi.StorageHandler'"
    val query2 = s"$baseQuery 'org.mamachi.StorageHandler' WITH SERDEPROPERTIES ('k1'='v1')"
    val e1 = intercept[ParseException] { parsePlan(query1) }
    val e2 = intercept[ParseException] { parsePlan(query2) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
  }

  test("create table - properties") {
    val query = "CREATE TABLE my_table (id int, name string) TBLPROPERTIES ('k1'='v1', 'k2'='v2')"
    parsePlan(query) match {
      case state: CreateTableStatement =>
        assert(state.properties == Map("k1" -> "v1", "k2" -> "v2"))
    }
  }

  test("create table(hive) - everything!") {
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
    assert(desc.schema == new StructType()
        .add("id", "int")
        .add("name", "string")
        .add("month", "int"))
    assert(desc.partitionColumnNames == Seq("month"))
    assert(desc.bucketSpec.isEmpty)
    assert(desc.viewText.isEmpty)
    assert(desc.viewCatalogAndNamespace.isEmpty)
    assert(desc.viewQueryColumnNames.isEmpty)
    assert(desc.storage.locationUri == Some(new URI("/path/to/mercury")))
    assert(desc.storage.inputFormat == Some("winput"))
    assert(desc.storage.outputFormat == Some("wowput"))
    assert(desc.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc.storage.properties == Map("k1" -> "v1"))
    assert(desc.properties == Map("k1" -> "v1", "k2" -> "v2"))
    assert(desc.comment == Some("no comment"))
  }

  // TODO: add tests for more commands.
}

object AsDataSourceV2Relation {
  def unapply(plan: LogicalPlan): Option[DataSourceV2Relation] = plan match {
    case SubqueryAlias(_, r: DataSourceV2Relation) => Some(r)
    case _ => None
  }
}
