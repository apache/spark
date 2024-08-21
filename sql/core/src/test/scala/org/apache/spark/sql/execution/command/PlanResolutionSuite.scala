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
import java.util.Collections

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, AnalysisTest, Analyzer, EmptyFunctionRegistry, NoSuchTableException, ResolvedFieldName, ResolvedIdentifier, ResolvedTable, ResolveSessionCatalog, UnresolvedAttribute, UnresolvedInlineTable, UnresolvedRelation, UnresolvedSubqueryColumnAliases, UnresolvedTable}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, EqualTo, Expression, InSubquery, IntegerLiteral, ListQuery, Literal, StringLiteral}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{AlterColumn, AnalysisOnlyCommand, AppendData, Assignment, CreateTable, CreateTableAsSelect, DeleteAction, DeleteFromTable, DescribeRelation, DropTable, InsertAction, InsertIntoStatement, LocalRelation, LogicalPlan, MergeIntoTable, OneRowRelation, OverwriteByExpression, OverwritePartitionsDynamic, Project, SetTableLocation, SetTableProperties, ShowTableProperties, SubqueryAlias, UnsetTableProperties, UpdateAction, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, Column, ColumnDefaultValue, Identifier, SupportsDelete, Table, TableCapability, TableCatalog, TableWritePrivilege, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.expressions.{LiteralValue, Transform}
import org.apache.spark.sql.execution.datasources.{CreateTable => CreateTableV1}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.{BooleanType, CharType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String

class PlanResolutionSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val v1Format = classOf[SimpleScanSource].getName
  private val v2Format = classOf[FakeV2Provider].getName

  private val table: Table = {
    val t = mock(classOf[SupportsDelete])
    when(t.columns()).thenReturn(
      Array(Column.create("i", IntegerType), Column.create("s", StringType)))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val table1: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(
      Array(Column.create("s", StringType), Column.create("i", IntegerType)))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val table2: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(
      Array(Column.create("i", IntegerType), Column.create("x", StringType, false)))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    when(t.name()).thenReturn("tab2")
    t
  }

  private val tableWithAcceptAnySchemaCapability: Table = {
    val t = mock(classOf[Table])
    when(t.name()).thenReturn("v2TableWithAcceptAnySchemaCapability")
    when(t.columns()).thenReturn(Array(Column.create("i", IntegerType)))
    when(t.capabilities()).thenReturn(Collections.singleton(TableCapability.ACCEPT_ANY_SCHEMA))
    t
  }

  private val charVarcharTable: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(
      Array(Column.create("c1", CharType(5)), Column.create("c2", VarcharType(5))))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val defaultValues: Table = {
    val t = mock(classOf[Table])
    val default1 = new ColumnDefaultValue("true", LiteralValue(true, BooleanType))
    val default2 = new ColumnDefaultValue("42", LiteralValue(42, IntegerType))
    when(t.columns()).thenReturn(Array(
      Column.create("i", BooleanType, true, null, default1, null),
      Column.create("s", IntegerType, true, null, default2, null)))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val defaultValues2: Table = {
    val t = mock(classOf[Table])
    val default = new ColumnDefaultValue(
      "'abc'", LiteralValue(UTF8String.fromString("abc"), StringType))
    when(t.columns()).thenReturn(Array(
      Column.create("i", StringType),
      Column.create("e", StringType, true, null, default, null)))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val tableWithColumnNamedDefault: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(Array(
      Column.create("s", StringType),
      Column.create("default", StringType)))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private def createV1TableMock(
      ident: Identifier,
      provider: String = v1Format,
      tableType: CatalogTableType = CatalogTableType.MANAGED): V1Table = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType()
      .add("i", "int")
      .add("s", "string")
      .add("point", new StructType().add("x", "int").add("y", "int")))
    when(t.tableType).thenReturn(tableType)
    when(t.provider).thenReturn(Some(provider))
    when(t.identifier).thenReturn(
      ident.asTableIdentifier.copy(catalog = Some(SESSION_CATALOG_NAME)))
    V1Table(t)
  }

  private val testCat: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArguments()(0).asInstanceOf[Identifier].name match {
        case "tab" => table
        case "tab1" => table1
        case "tab2" => table2
        case "charvarchar" => charVarcharTable
        case "defaultvalues" => defaultValues
        case "defaultvalues2" => defaultValues2
        case "tablewithcolumnnameddefault" => tableWithColumnNamedDefault
        case "v2TableWithAcceptAnySchemaCapability" => tableWithAcceptAnySchemaCapability
        case name => throw new NoSuchTableException(Seq(name))
      }
    })
    when(newCatalog.loadTable(any(), any[java.util.Set[TableWritePrivilege]]()))
      .thenCallRealMethod()
    when(newCatalog.name()).thenReturn("testcat")
    newCatalog
  }

  private val v2SessionCatalog: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      val ident = invocation.getArguments()(0).asInstanceOf[Identifier]
      ident.name match {
        case "v1Table" | "v1Table1" => createV1TableMock(ident)
        case "v1HiveTable" => createV1TableMock(ident, provider = "hive")
        case "v2Table" => table
        case "v2Table1" => table1
        case "view" => createV1TableMock(ident, tableType = CatalogTableType.VIEW)
        case name => throw new NoSuchTableException(Seq(name))
      }
    })
    when(newCatalog.loadTable(any(), any[java.util.Set[TableWritePrivilege]]()))
      .thenCallRealMethod()
    when(newCatalog.name()).thenReturn(CatalogManager.SESSION_CATALOG_NAME)
    newCatalog
  }

  private val v1SessionCatalog: SessionCatalog = new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))
  createTempView(v1SessionCatalog, "v", LocalRelation(Nil), false)

  private val catalogManagerWithDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArguments()(0).asInstanceOf[String] match {
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
      invocation.getArguments()(0).asInstanceOf[String] match {
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

  def parseAndResolve(
      query: String,
      withDefault: Boolean = false,
      checkAnalysis: Boolean = false): LogicalPlan = {
    val catalogManager = if (withDefault) {
      catalogManagerWithDefault
    } else {
      catalogManagerWithoutDefault
    }
    val analyzer = new Analyzer(catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(
        new ResolveSessionCatalog(catalogManager))
    }
    // We don't check analysis here by default, as we expect the plan to be unresolved
    // such as `CreateTable`.
    val analyzed = analyzer.execute(CatalystSqlParser.parsePlan(query))
    if (checkAnalysis) {
      analyzer.checkAnalysis(analyzed)
    }
    analyzed
  }

  private def parseResolveCompare(query: String, expected: LogicalPlan): Unit =
    comparePlans(parseAndResolve(query), expected, checkAnalysis = true)

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parseAndResolve(sql).collect {
      case CreateTableV1(tableDesc, mode, _) => (tableDesc, mode == SaveMode.Ignore)
    }.head
  }

  private def assertUnsupported(
      sql: String,
      parameters: Map[String, String],
      context: ExpectedContext): Unit = {
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sql)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = parameters,
      context = context
    )
  }

  test("create table - with partitioned by") {
    val query = "CREATE TABLE my_tab(a INT comment 'test', b STRING) " +
        "USING parquet PARTITIONED BY (a)"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab", Some("default"), Some(SESSION_CATALOG_NAME)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
          .add("a", IntegerType, nullable = true, "test")
          .add("b", StringType),
      provider = Some("parquet"),
      partitionColumnNames = Seq("a")
    )

    parseAndResolve(query) match {
      case CreateTableV1(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - partitioned by transforms") {
    val transforms = Seq("years(ts)", "months(ts)", "days(ts)", "hours(ts)", "foo(a, 'bar', 34)")
    transforms.foreach { transform =>
      val query =
        s"""
           |CREATE TABLE my_tab(a INT, b STRING) USING parquet
           |PARTITIONED BY ($transform)
           """.stripMargin
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          parseAndResolve(query)
        },
        errorClass = "_LEGACY_ERROR_TEMP_2067",
        parameters = Map("transform" -> transform))
    }
  }

  test("create table - partitioned by multiple bucket transforms") {
    val transforms = Seq("bucket(32, b), sorted_bucket(b, 32, c)")
    transforms.foreach { transform =>
      val query =
        s"""
           |CREATE TABLE my_tab(a INT, b STRING, c String) USING parquet
           |PARTITIONED BY ($transform)
           """.stripMargin
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          parseAndResolve(query)
        },
        errorClass = "UNSUPPORTED_FEATURE.MULTIPLE_BUCKET_TRANSFORMS",
        parameters = Map.empty)
    }
  }

  test("create table - with bucket") {
    val query = "CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
        "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab", Some("default"), Some(SESSION_CATALOG_NAME)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"),
      bucketSpec = Some(BucketSpec(5, Seq("a"), Seq("b")))
    )

    parseAndResolve(query) match {
      case CreateTableV1(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - with comment") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab", Some("default"), Some(SESSION_CATALOG_NAME)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"),
      comment = Some("abc"))

    parseAndResolve(sql) match {
      case CreateTableV1(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with table properties") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet TBLPROPERTIES('test' = 'test')"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab", Some("default"), Some(SESSION_CATALOG_NAME)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"),
      properties = Map("test" -> "test"))

    parseAndResolve(sql) match {
      case CreateTableV1(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with location") {
    val v1 = "CREATE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("my_tab", Some("default"), Some(SESSION_CATALOG_NAME)),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(new URI("/tmp/file"))),
      schema = new StructType().add("a", IntegerType).add("b", StringType),
      provider = Some("parquet"))

    parseAndResolve(v1) match {
      case CreateTableV1(tableDesc, _, None) =>
        assert(tableDesc == expectedTableDesc.copy(createTime = tableDesc.createTime))
      case other =>
        fail(s"Expected to parse ${classOf[CreateTableCommand].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $v1")
    }

    val v2 =
      """CREATE TABLE my_tab(a INT, b STRING)
        |USING parquet
        |OPTIONS (path '/tmp/file')
        |LOCATION '/tmp/file'""".stripMargin
    checkError(
      exception = intercept[ParseException] {
        parseAndResolve(v2)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0032",
      parameters = Map("pathOne" -> "/tmp/file", "pathTwo" -> "/tmp/file"),
      context = ExpectedContext(
        fragment = v2,
        start = 0,
        stop = 97))
  }

  test("create table - byte length literal table name") {
    val sql = "CREATE TABLE 1m.2g(a INT) USING parquet"

    val expectedTableDesc = CatalogTable(
      identifier = TableIdentifier("2g", Some("1m"), Some(SESSION_CATALOG_NAME)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", IntegerType),
      provider = Some("parquet"))

    parseAndResolve(sql) match {
      case CreateTableV1(tableDesc, _, None) =>
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
      identifier = TableIdentifier("table_name", Some("default"), Some(SESSION_CATALOG_NAME)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        properties = Map("a" -> "1", "b" -> "0.1", "c" -> "true")
      ),
      schema = new StructType,
      provider = Some("json")
    )

    parseAndResolve(sql) match {
      case CreateTableV1(tableDesc, _, None) =>
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
      assert(desc.identifier.catalog.contains(SESSION_CATALOG_NAME))
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

    parseAndResolve(sql) match {
      case create: CreateTable =>
        assert(create.name.asInstanceOf[ResolvedIdentifier].catalog.name == "testcat")
        assert(create.name.asInstanceOf[ResolvedIdentifier].identifier.toString ==
          "mydb.table_name")
        assert(create.tableSchema == new StructType()
            .add("id", LongType)
            .add("description", StringType)
            .add("point", new StructType().add("x", DoubleType).add("y", DoubleType)))
        assert(create.partitioning.isEmpty)
        assert(create.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTable].getName} from query," +
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

    parseAndResolve(sql, withDefault = true) match {
      case create: CreateTable =>
        assert(create.name.asInstanceOf[ResolvedIdentifier].catalog.name == "testcat")
        assert(create.name.asInstanceOf[ResolvedIdentifier].identifier.toString ==
          "mydb.table_name")
        assert(create.tableSchema == new StructType()
            .add("id", LongType)
            .add("description", StringType)
            .add("point", new StructType().add("x", DoubleType).add("y", DoubleType)))
        assert(create.partitioning.isEmpty)
        assert(create.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTable].getName} from query," +
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

    parseAndResolve(sql) match {
      case create: CreateTable =>
        assert(create.name.asInstanceOf[ResolvedIdentifier].catalog.name ==
          CatalogManager.SESSION_CATALOG_NAME)
        assert(create.name.asInstanceOf[ResolvedIdentifier].identifier.toString ==
          "mydb.page_view")
        assert(create.tableSchema == new StructType()
            .add("id", LongType)
            .add("description", StringType)
            .add("point", new StructType().add("x", DoubleType).add("y", DoubleType)))
        assert(create.partitioning.isEmpty)
        assert(create.ignoreIfExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTable].getName} from query," +
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

    parseAndResolve(sql) match {
      case ctas: CreateTableAsSelect =>
        assert(ctas.name.asInstanceOf[ResolvedIdentifier].catalog.name == "testcat")
        assert(
          ctas.name.asInstanceOf[ResolvedIdentifier].identifier.toString == "mydb.table_name"
        )
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

    parseAndResolve(sql, withDefault = true) match {
      case ctas: CreateTableAsSelect =>
        assert(ctas.name.asInstanceOf[ResolvedIdentifier].catalog.name == "testcat")
        assert(
          ctas.name.asInstanceOf[ResolvedIdentifier].identifier.toString == "mydb.table_name"
        )
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

    parseAndResolve(sql) match {
      case ctas: CreateTableAsSelect =>
        assert(ctas.name.asInstanceOf[ResolvedIdentifier].catalog.name ==
          CatalogManager.SESSION_CATALOG_NAME)
        assert(ctas.name.asInstanceOf[ResolvedIdentifier].identifier.toString ==
          "mydb.page_view")
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
    val tableIdent1 = ResolvedIdentifier(v2SessionCatalog, Identifier.of(Array("db"), "v1Table"))
    val tableName2 = "v1Table"
    val tableIdent2 = ResolvedIdentifier(v2SessionCatalog, Identifier.of(Array("default"),
      "v1Table"))

    parseResolveCompare(s"DROP TABLE $tableName1",
      DropTable(tableIdent1, ifExists = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName1",
      DropTable(tableIdent1, ifExists = true, purge = false))
    parseResolveCompare(s"DROP TABLE $tableName2",
      DropTable(tableIdent2, ifExists = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName2",
      DropTable(tableIdent2, ifExists = true, purge = false))
    parseResolveCompare(s"DROP TABLE $tableName2 PURGE",
      DropTable(tableIdent2, ifExists = false, purge = true))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName2 PURGE",
      DropTable(tableIdent2, ifExists = true, purge = true))
  }

  test("drop table in v2 catalog") {
    val tableName1 = "testcat.db.tab"
    val tableIdent1 = Identifier.of(Array("db"), "tab")
    val tableName2 = "testcat.tab"
    val tableIdent2 = Identifier.of(Array.empty, "tab")

    parseResolveCompare(s"DROP TABLE $tableName1",
      DropTable(ResolvedIdentifier(testCat, tableIdent1), ifExists = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName1",
      DropTable(ResolvedIdentifier(testCat, tableIdent1), ifExists = true, purge = false))
    parseResolveCompare(s"DROP TABLE $tableName2",
      DropTable(ResolvedIdentifier(testCat, tableIdent2), ifExists = false, purge = false))
    parseResolveCompare(s"DROP TABLE IF EXISTS $tableName2",
      DropTable(ResolvedIdentifier(testCat, tableIdent2), ifExists = true, purge = false))
  }

  test("drop view") {
    val viewName1 = "db.view"
    val viewIdent1 = TableIdentifier("view", Option("db"), Some(SESSION_CATALOG_NAME))
    val viewName2 = "view"
    val viewIdent2 = TableIdentifier("view", Option("default"), Some(SESSION_CATALOG_NAME))
    val tempViewName = "v"
    val tempViewIdent = Identifier.of(Array.empty, "v")

    parseResolveCompare(s"DROP VIEW $viewName1",
      DropTableCommand(viewIdent1, ifExists = false, isView = true, purge = false))
    parseResolveCompare(s"DROP VIEW IF EXISTS $viewName1",
      DropTableCommand(viewIdent1, ifExists = true, isView = true, purge = false))
    parseResolveCompare(s"DROP VIEW $viewName2",
      DropTableCommand(viewIdent2, ifExists = false, isView = true, purge = false))
    parseResolveCompare(s"DROP VIEW IF EXISTS $viewName2",
      DropTableCommand(viewIdent2, ifExists = true, isView = true, purge = false))
    parseResolveCompare(s"DROP VIEW $tempViewName",
      DropTempViewCommand(tempViewIdent))
    parseResolveCompare(s"DROP VIEW IF EXISTS $tempViewName",
      DropTempViewCommand(tempViewIdent))
  }

  test("drop view in v2 catalog") {
    val e = intercept[AnalysisException] {
      parseAndResolve("DROP VIEW testcat.db.view", checkAnalysis = true)
    }
    checkError(
      e,
      errorClass = "UNSUPPORTED_FEATURE.CATALOG_OPERATION",
      parameters = Map("catalogName" -> "`testcat`", "operation" -> "views"))
  }

  // ALTER VIEW view_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER VIEW view_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter view: alter view properties") {
    val sql1_view = "ALTER VIEW view SET TBLPROPERTIES ('test' = 'test', " +
        "'comment' = 'new_comment')"
    val sql2_view = "ALTER VIEW view UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_view = "ALTER VIEW view UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

    val parsed1_view = parseAndResolve(sql1_view)
    val parsed2_view = parseAndResolve(sql2_view)
    val parsed3_view = parseAndResolve(sql3_view)

    val tableIdent = TableIdentifier("view", Some("default"), Some(SESSION_CATALOG_NAME))
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
          val tableIdent = TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME))
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
            case SetTableProperties(_: ResolvedTable, properties) =>
              assert(properties == Map(("test", "test"), ("comment", "new_comment")))
            case _ => fail(s"expect ${SetTableProperties.getClass.getName}")
          }

          parsed2 match {
            case UnsetTableProperties(_: ResolvedTable, propertyKeys, ifExists) =>
              assert(propertyKeys == Seq("comment", "test"))
              assert(!ifExists)
            case _ => fail(s"expect ${UnsetTableProperties.getClass.getName}")
          }

          parsed3 match {
            case UnsetTableProperties(_: ResolvedTable, propertyKeys, ifExists) =>
              assert(propertyKeys == Seq("comment", "test"))
              assert(ifExists)
            case _ => fail(s"expect ${UnsetTableProperties.getClass.getName}")
          }
        }
    }

    val sql4 = "ALTER TABLE non_exist SET TBLPROPERTIES ('test' = 'test')"
    val sql5 = "ALTER TABLE non_exist UNSET TBLPROPERTIES ('test')"
    val parsed4 = parseAndResolve(sql4)
    val parsed5 = parseAndResolve(sql5)

    // For non-existing tables, we convert it to v2 command with `UnresolvedV2Table`
    parsed4 match {
      case SetTableProperties(_: UnresolvedTable, _) => // OK
      case _ =>
        fail(s"Expect ${SetTableProperties.getClass.getName}, but got:\n" + parsed4.treeString)
    }
    parsed5 match {
      case UnsetTableProperties(_: UnresolvedTable, _, _) => // OK
      case _ =>
        fail(s"Expect ${UnsetTableProperties.getClass.getName}, but got:\n" + parsed5.treeString)
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
            TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME)),
            Map("a" -> "1", "b" -> "0.1", "c" -> "true"),
            isView = false)

          comparePlans(parsed, expected)
        } else {
          parsed match {
            case SetTableProperties(_: ResolvedTable, changes) =>
              assert(changes == Map(("a", "1"), ("b", "0.1"), ("c", "true")))
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
            TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME)),
            None,
            "new location")
          comparePlans(parsed, expected)
        } else {
          parsed match {
            case SetTableLocation(_: ResolvedTable, _, location) =>
              assert(location === "new location")
            case _ =>
              fail(s"Expect ${SetTableLocation.getClass.getName}, but got:\n" + parsed.treeString)
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
            TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME)),
            Map.empty, false, parsed1.output)
          val expected2 = DescribeTableCommand(
            TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME)),
            Map.empty, true, parsed2.output)

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
            TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME)),
            Map("a" -> "1"), false, parsed3.output)
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
        case DeleteFromTable(AsDataSourceV2Relation(_), Literal.TrueLiteral) =>
        case _ => fail("Expect DeleteFromTable, but got:\n" + parsed1.treeString)
      }

      parsed2 match {
        case DeleteFromTable(
          AsDataSourceV2Relation(_),
          EqualTo(name: UnresolvedAttribute, StringLiteral("Robert"))) =>
          assert(name.name == "name")
        case _ => fail("Expect DeleteFromTable, but got:\n" + parsed2.treeString)
      }

      parsed3 match {
        case DeleteFromTable(
          SubqueryAlias(AliasIdentifier("t", Seq()), AsDataSourceV2Relation(_)),
          EqualTo(name: UnresolvedAttribute, StringLiteral("Robert"))) =>
          assert(name.name == "t.name")
        case _ => fail("Expect DeleteFromTable, but got:\n" + parsed3.treeString)
      }

      parsed4 match {
        case DeleteFromTable(
            SubqueryAlias(AliasIdentifier("t", Seq()), AsDataSourceV2Relation(_)),
            InSubquery(values, query)) =>
          assert(values.size == 1 && values.head.isInstanceOf[UnresolvedAttribute])
          assert(values.head.asInstanceOf[UnresolvedAttribute].name == "t.name")
          query match {
            case ListQuery(Project(projects, SubqueryAlias(AliasIdentifier("s", Seq()),
                UnresolvedSubqueryColumnAliases(outputColumnNames, Project(_, _: OneRowRelation)))),
                _, _, _, _, _) =>
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
      val sql5 = s"UPDATE $tblName SET name=DEFAULT, age=DEFAULT"
      // Note: 'i' and 's' are the names of the columns in 'tblName'.
      val sql6 = s"UPDATE $tblName SET i=DEFAULT, s=DEFAULT"

      val parsed1 = parseAndResolve(sql1)
      val parsed2 = parseAndResolve(sql2)
      val parsed3 = parseAndResolve(sql3)
      val parsed4 = parseAndResolve(sql4)
      val parsed5 = parseAndResolve(sql5)
      val parsed6 = parseAndResolve(sql6)

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
                _, _, _, _, _) =>
              assert(projects.size == 1 && projects.head.name == "s.name")
              assert(outputColumnNames.size == 1 && outputColumnNames.head == "name")
            case o => fail("Unexpected subquery: \n" + o.treeString)
          }

        case _ => fail("Expect UpdateTable, but got:\n" + parsed4.treeString)
      }

      parsed5 match {
        case UpdateTable(
          AsDataSourceV2Relation(_),
          Seq(
            Assignment(name: UnresolvedAttribute, UnresolvedAttribute(Seq("DEFAULT"))),
            Assignment(age: UnresolvedAttribute, UnresolvedAttribute(Seq("DEFAULT")))),
          None) =>
          assert(name.name == "name")
          assert(age.name == "age")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed5.treeString)
      }

      parsed6 match {
        case UpdateTable(
          AsDataSourceV2Relation(_),
          Seq(
            // Note that when resolving DEFAULT column references, the analyzer will insert literal
            // NULL values if the corresponding table does not define an explicit default value for
            // that column. This is intended.
            Assignment(i: AttributeReference, Literal(null, IntegerType)),
            Assignment(s: AttributeReference, Literal(null, StringType))),
          None) =>
          assert(i.name == "i")
          assert(s.name == "s")

        case _ => fail("Expect UpdateTable, but got:\n" + parsed6.treeString)
      }
    }

    val sql1 = "UPDATE non_existing SET id=1"
    val parsed1 = parseAndResolve(sql1)
    parsed1 match {
      case u: UpdateTable =>
        assert(u.table.isInstanceOf[UnresolvedRelation])
      case _ => fail("Expect UpdateTable, but got:\n" + parsed1.treeString)
    }

    val sql2 = "UPDATE testcat.charvarchar SET c1='a', c2=1"
    val parsed2 = parseAndResolve(sql2)
    parsed2 match {
      case u: UpdateTable =>
        assert(u.assignments.length == 2)
        u.assignments(0).value match {
          case s: StaticInvoke =>
            assert(s.arguments.length == 2)
            assert(s.functionName == "charTypeWriteSideCheck")
          case other => fail("Expect StaticInvoke, but got: " + other)
        }
        u.assignments(1).value match {
          case s: StaticInvoke =>
            assert(s.arguments.length == 2)
            assert(s.arguments.head.isInstanceOf[Cast])
            val cast = s.arguments.head.asInstanceOf[Cast]
            assert(cast.getTagValue(Cast.BY_TABLE_INSERTION).isDefined)
            assert(s.functionName == "varcharTypeWriteSideCheck")
          case other => fail("Expect StaticInvoke, but got: " + other)
        }
      case _ => fail("Expect UpdateTable, but got:\n" + parsed2.treeString)
    }

    val sql3 = "UPDATE testcat.defaultvalues SET i=DEFAULT, s=DEFAULT"
    val sql4 = "UPDATE testcat.defaultvalues2 SET i=DEFAULT"
    // Table with ACCEPT_ANY_SCHEMA can also resolve the column DEFAULT.
    val sql5 = "UPDATE testcat.v2TableWithAcceptAnySchemaCapability SET i=DEFAULT"

    val parsed3 = parseAndResolve(sql3)
    val parsed4 = parseAndResolve(sql4)
    val parsed5 = parseAndResolve(sql5)

    parsed3 match {
      case UpdateTable(
      _,
      Seq(
      Assignment(i: AttributeReference, Literal(true, BooleanType)),
      Assignment(s: AttributeReference, Literal(42, IntegerType))),
      None) =>
        assert(i.name == "i")
        assert(s.name == "s")

      case _ => fail("Expect UpdateTable, but got:\n" + parsed3.treeString)
    }

    parsed4 match {
      case UpdateTable(
      _,
      Seq(Assignment(i: AttributeReference, Literal(null, StringType))),
      None) =>
        assert(i.name == "i")

      case _ => fail("Expect UpdateTable, but got:\n" + parsed4.treeString)
    }

    parsed5 match {
      case UpdateTable(
      _,
      Seq(Assignment(i: AttributeReference, Literal(null, IntegerType))),
      None) =>
        assert(i.name == "i")

      case _ => fail("Expect UpdateTable, but got:\n" + parsed5.treeString)
    }

    // Negative cases.
    // UPDATE condition won't resolve column "DEFAULT"
    val sql6 = "UPDATE testcat.defaultvalues SET i=DEFAULT, s=DEFAULT WHERE i=DEFAULT"
    checkError(
      exception = intercept[AnalysisException] {
        parseAndResolve(sql6, checkAnalysis = true)
      },
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`DEFAULT`", "proposal" -> "`i`, `s`"),
      context = ExpectedContext(
        fragment = "DEFAULT",
        start = 62,
        stop = 68))

    val sql7 = "UPDATE testcat.tab2 SET x=DEFAULT"
    checkError(
      exception = intercept[AnalysisException] {
        parseAndResolve(sql7, checkAnalysis = true)
      },
      errorClass = "NO_DEFAULT_COLUMN_VALUE_AVAILABLE",
      parameters = Map("colName" -> "`x`")
    )
  }

  test("SPARK-38869 INSERT INTO table with ACCEPT_ANY_SCHEMA capability") {
    // Note: 'i' is the correct column name, but since the table has ACCEPT_ANY_SCHEMA capability,
    // DEFAULT column resolution should skip this table.
    val sql1 = s"INSERT INTO v2TableWithAcceptAnySchemaCapability VALUES(DEFAULT)"
    val sql2 = s"INSERT INTO v2TableWithAcceptAnySchemaCapability SELECT DEFAULT"
    val parsed1 = parseAndResolve(sql1)
    val parsed2 = parseAndResolve(sql2)
    parsed1 match {
      case InsertIntoStatement(
        _, _, _,
        UnresolvedInlineTable(_, Seq(Seq(UnresolvedAttribute(Seq("DEFAULT"))))),
        _, _, _) =>

      case _ => fail("Expect UpdateTable, but got:\n" + parsed1.treeString)
    }
    parsed2 match {
      case InsertIntoStatement(
        _, _, _,
        Project(Seq(UnresolvedAttribute(Seq("DEFAULT"))), _),
        _, _, _) =>

      case _ => fail("Expect UpdateTable, but got:\n" + parsed1.treeString)
    }
  }

  test("INSERT INTO table with default column value") {
    val sql1 = "INSERT INTO testcat.defaultvalues VALUES (DEFAULT, DEFAULT)"
    parseAndResolve(sql1) match {
      // The top-most Project just adds aliases.
      case AppendData(_: DataSourceV2Relation, Project(_, l: LocalRelation), _, _, _, _) =>
        assert(l.data.length == 1)
        val row = l.data.head
        assert(row.numFields == 2)
        assert(row.getBoolean(0) == true)
        assert(row.getInt(1) == 42)
      case other => fail("Expected AppendData, but got:\n" + other.treeString)
    }

    val sql2 = "INSERT INTO testcat.tab2 VALUES (1, DEFAULT)"
    checkError(
      exception = intercept[AnalysisException] {
        parseAndResolve(sql2, checkAnalysis = true)
      },
      errorClass = "NO_DEFAULT_COLUMN_VALUE_AVAILABLE",
      parameters = Map("colName" -> "`x`")
    )

    val sql3 = "INSERT INTO testcat.tab2 VALUES (1)"
    checkError(
      exception = intercept[AnalysisException] {
        parseAndResolve(sql3, checkAnalysis = true)
      },
      errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
      parameters = Map(
        "tableName" -> "`tab2`",
        "tableColumns" -> "`i`, `x`",
        "dataColumns" -> "`col1`")
    )
  }

  test("InsertIntoStatement byName") {
    val tblName = "testcat.tab1"
    val insertSql = s"INSERT INTO $tblName(i, s) VALUES (3, 'a')"
    val insertParsed = parseAndResolve(insertSql)
    val overwriteSql = s"INSERT OVERWRITE $tblName(i, s) VALUES (3, 'a')"
    val overwriteParsed = parseAndResolve(overwriteSql)
    insertParsed match {
      case AppendData(_: DataSourceV2Relation, _, _, isByName, _, _) =>
        assert(isByName)
      case _ => fail("Expected AppendData, but got:\n" + insertParsed.treeString)
    }
    overwriteParsed match {
      case OverwriteByExpression(_: DataSourceV2Relation, _, _, _, isByName, _, _) =>
        assert(isByName)
      case _ => fail("Expected OverwriteByExpression, but got:\n" + overwriteParsed.treeString)
    }
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val dynamicOverwriteParsed = parseAndResolve(overwriteSql)
      dynamicOverwriteParsed match {
        case OverwritePartitionsDynamic(_: DataSourceV2Relation, _, _, isByName, _) =>
          assert(isByName)
        case _ =>
          fail("Expected OverwriteByExpression, but got:\n" + dynamicOverwriteParsed.treeString)
      }
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
          val tableIdent = TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME))
          val oldColumn = StructField("i", IntegerType)
          val newColumn = StructField("i", LongType)
          val expected1 = AlterTableChangeColumnCommand(
            tableIdent, "i", newColumn)
          val expected2 = AlterTableChangeColumnCommand(
            tableIdent, "i", oldColumn.withComment("new comment"))

          comparePlans(parsed1, expected1)
          comparePlans(parsed2, expected2)

          val sql3 = s"ALTER TABLE $tblName ALTER COLUMN j COMMENT 'new comment'"
          checkError(
            exception = intercept[AnalysisException] {
              parseAndResolve(sql3)
            },
            errorClass = "_LEGACY_ERROR_TEMP_1331",
            parameters = Map(
              "fieldName" -> "j",
              "table" -> "spark_catalog.default.v1Table",
              "schema" ->
                """root
                  | |-- i: integer (nullable = true)
                  | |-- s: string (nullable = true)
                  | |-- point: struct (nullable = true)
                  | |    |-- x: integer (nullable = true)
                  | |    |-- y: integer (nullable = true)
                  |""".stripMargin),
            context = ExpectedContext(fragment = sql3, start = 0, stop = 55))

          val sql4 = s"ALTER TABLE $tblName ALTER COLUMN point.x TYPE bigint"
          val e2 = intercept[AnalysisException] {
            parseAndResolve(sql4)
          }
          checkError(
            exception = e2,
            errorClass = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
            sqlState = "0A000",
            parameters = Map("tableName" -> "`spark_catalog`.`default`.`v1Table`",
              "operation" -> "ALTER COLUMN with qualified column"))
        } else {
          parsed1 match {
            case AlterColumn(
                _: ResolvedTable,
                column: ResolvedFieldName,
                Some(LongType),
                None,
                None,
                None,
                None) =>
              assert(column.name == Seq("i"))
            case _ => fail("expect AlterTableAlterColumn")
          }

          parsed2 match {
            case AlterColumn(
                _: ResolvedTable,
                column: ResolvedFieldName,
                None,
                None,
                Some("new comment"),
                None,
                None) =>
              assert(column.name == Seq("i"))
            case _ => fail("expect AlterTableAlterColumn")
          }
        }
    }

    val sql = s"ALTER TABLE v1HiveTable ALTER COLUMN i TYPE char(1)"
    val newColumnWithCleanedType = StructField("i", CharType(1), true)
    val expected = AlterTableChangeColumnCommand(
      TableIdentifier("v1HiveTable", Some("default"), Some(SESSION_CATALOG_NAME)),
      "i", newColumnWithCleanedType)
    val parsed = parseAndResolve(sql)
    comparePlans(parsed, expected)
  }

  test("alter table: alter column action is not specified") {
    val sql = "ALTER TABLE v1Table ALTER COLUMN i"
    checkError(
      exception = intercept[ParseException] {
        parseAndResolve(sql)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        "ALTER TABLE table ALTER COLUMN requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER"),
      context = ExpectedContext(fragment = sql, start = 0, stop = 33))
  }

  test("alter table: alter column case sensitivity for v1 table") {
    val tblName = "v1Table"
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val sql = s"ALTER TABLE $tblName ALTER COLUMN I COMMENT 'new comment'"
        if (caseSensitive) {
          checkError(
            exception = intercept[AnalysisException] {
              parseAndResolve(sql)
            },
            errorClass = "_LEGACY_ERROR_TEMP_1331",
            parameters = Map(
              "fieldName" -> "I",
              "table" -> "spark_catalog.default.v1Table",
              "schema" ->
                """root
                  | |-- i: integer (nullable = true)
                  | |-- s: string (nullable = true)
                  | |-- point: struct (nullable = true)
                  | |    |-- x: integer (nullable = true)
                  | |    |-- y: integer (nullable = true)
                  |""".stripMargin),
            context = ExpectedContext(fragment = sql, start = 0, stop = 55))
        } else {
          val actual = parseAndResolve(sql)
          val expected = AlterTableChangeColumnCommand(
            TableIdentifier(tblName, Some("default"), Some(SESSION_CATALOG_NAME)),
            "i",
            StructField("i", IntegerType).withComment("new comment"))
          comparePlans(actual, expected)
        }
      }
    }
  }

  test("alter table: hive style change column") {
    Seq("v2Table", "testcat.tab").foreach { tblName =>
      parseAndResolve(s"ALTER TABLE $tblName CHANGE COLUMN i i int COMMENT 'an index'") match {
        case AlterColumn(
            _: ResolvedTable, _: ResolvedFieldName, None, None, Some(comment), None, None) =>
          assert(comment == "an index")
        case _ => fail("expect AlterTableAlterColumn with comment change only")
      }

      parseAndResolve(s"ALTER TABLE $tblName CHANGE COLUMN i i long COMMENT 'an index'") match {
        case AlterColumn(
            _: ResolvedTable, _: ResolvedFieldName, Some(dataType), None, Some(comment), None,
            None) =>
          assert(comment == "an index")
          assert(dataType == LongType)
        case _ => fail("expect AlterTableAlterColumn with type and comment changes")
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
      val catalog = if (isSessionCatalog) v2SessionCatalog else testCat
      val tableIdent = if (isSessionCatalog) "v2Table" else "tab"
      parsed match {
        case AlterColumn(r: ResolvedTable, _, _, _, _, _, _) =>
          assert(r.catalog == catalog)
          assert(r.identifier.name() == tableIdent)
        case Project(_, AsDataSourceV2Relation(r)) =>
          assert(r.catalog.exists(_ == catalog))
          assert(r.identifier.exists(_.name() == tableIdent))
        case AppendData(r: DataSourceV2Relation, _, _, _, _, _) =>
          assert(r.catalog.exists(_ == catalog))
          assert(r.identifier.exists(_.name() == tableIdent))
        case DescribeRelation(r: ResolvedTable, _, _, _) =>
          assert(r.catalog == catalog)
          assert(r.identifier.name() == tableIdent)
        case ShowTableProperties(r: ResolvedTable, _, _) =>
          assert(r.catalog == catalog)
          assert(r.identifier.name() == tableIdent)
        case ShowTablePropertiesCommand(t: TableIdentifier, _, _) =>
          assert(t.identifier == tableIdent)
      }
    }
  }

  test("MERGE INTO TABLE - primary") {
    def getAttributes(plan: LogicalPlan): (AttributeReference, AttributeReference) =
      (plan.output.find(_.name == "i").get.asInstanceOf[AttributeReference],
        plan.output.find(_.name == "s").get.asInstanceOf[AttributeReference])

    def checkMergeConditionResolution(
        target: LogicalPlan,
        source: LogicalPlan,
        mergeCondition: Expression): Unit = {
      val (si, _) = getAttributes(source)
      val (ti, _) = getAttributes(target)
      mergeCondition match {
        case EqualTo(l: AttributeReference, r: AttributeReference) =>
          assert(l.sameRef(ti) && r.sameRef(si))
        case Literal(_, BooleanType) => // this is acceptable as a merge condition
        case other => fail("unexpected merge condition " + other)
      }
    }

    def checkMatchedClausesResolution(
        target: LogicalPlan,
        source: LogicalPlan,
        deleteCondAttr: Option[AttributeReference],
        updateCondAttr: Option[AttributeReference],
        updateAssigns: Seq[Assignment],
        starInUpdate: Boolean = false): Unit = {
      val (si, ss) = getAttributes(source)
      val (ti, ts) = getAttributes(target)
      deleteCondAttr.foreach(a => assert(a.sameRef(ts)))
      updateCondAttr.foreach(a => assert(a.sameRef(ts)))

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
    }

    def checkNotMatchedClausesResolution(
        target: LogicalPlan,
        source: LogicalPlan,
        insertCondAttr: Option[AttributeReference],
        insertAssigns: Seq[Assignment]): Unit = {
      val (si, ss) = getAttributes(source)
      val (ti, ts) = getAttributes(target)
      insertCondAttr.foreach(a => assert(a.sameRef(ss)))
      assert(insertAssigns.size == 2)
      assert(insertAssigns(0).key.asInstanceOf[AttributeReference].sameRef(ti))
      assert(insertAssigns(0).value.asInstanceOf[AttributeReference].sameRef(si))
      assert(insertAssigns(1).key.asInstanceOf[AttributeReference].sameRef(ts))
      assert(insertAssigns(1).value.asInstanceOf[AttributeReference].sameRef(ss))
    }

    def checkNotMatchedBySourceClausesResolution(
        target: LogicalPlan,
        deleteCondAttr: Option[AttributeReference],
        updateCondAttr: Option[AttributeReference],
        updateAssigns: Seq[Assignment]): Unit = {
      val (_, ts) = getAttributes(target)
      deleteCondAttr.foreach(a => assert(a.sameRef(ts)))
      updateCondAttr.foreach(a => assert(a.sameRef(ts)))
      assert(updateAssigns.size == 1)
      assert(updateAssigns.head.key.asInstanceOf[AttributeReference].sameRef(ts))
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
             |WHEN NOT MATCHED BY SOURCE AND (target.s='delete') THEN DELETE
             |WHEN NOT MATCHED BY SOURCE AND (target.s='update') THEN UPDATE SET target.s = 'delete'
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
                  insertAssigns)),
              Seq(DeleteAction(Some(EqualTo(ndl: AttributeReference, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(nul: AttributeReference, StringLiteral("update"))),
                  notMatchedBySourceUpdateAssigns))) =>
            checkMergeConditionResolution(target, source, mergeCondition)
            checkMatchedClausesResolution(target, source, Some(dl), Some(ul), updateAssigns)
            checkNotMatchedClausesResolution(target, source, Some(il), insertAssigns)
            checkNotMatchedBySourceClausesResolution(target, Some(ndl), Some(nul),
              notMatchedBySourceUpdateAssigns)

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
                  insertAssigns)),
              Seq()) =>
            checkMergeConditionResolution(target, source, mergeCondition)
            checkMatchedClausesResolution(target, source, Some(dl), Some(ul), updateAssigns,
              starInUpdate = true)
            checkNotMatchedClausesResolution(target, source, Some(il), insertAssigns)

          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }

        // merge with star should get resolved into specific actions even if there
        // is no other unresolved expression in the merge
        parseAndResolve(
          s"""
             |MERGE INTO $target AS target
             |USING $source AS source
             |ON true
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
           """.stripMargin) match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(target)),
              SubqueryAlias(AliasIdentifier("source", Seq()), AsDataSourceV2Relation(source)),
              mergeCondition,
              Seq(UpdateAction(None, updateAssigns)),
              Seq(InsertAction(None, insertAssigns)),
              Seq()) =>
            checkMergeConditionResolution(target, source, mergeCondition)
            checkMatchedClausesResolution(target, source, None, None, updateAssigns,
              starInUpdate = true)
            checkNotMatchedClausesResolution(target, source, None, insertAssigns)

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
             |WHEN NOT MATCHED BY SOURCE AND (target.s='delete') THEN DELETE
             |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET target.s = 'delete'
           """.stripMargin
        parseAndResolve(sql3) match {
          case MergeIntoTable(
              SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(target)),
              SubqueryAlias(AliasIdentifier("source", Seq()), AsDataSourceV2Relation(source)),
              mergeCondition,
              Seq(DeleteAction(Some(_)), UpdateAction(None, updateAssigns)),
              Seq(InsertAction(None, insertAssigns)),
              Seq(DeleteAction(Some(EqualTo(_: AttributeReference, StringLiteral("delete")))),
                UpdateAction(None, notMatchedBySourceUpdateAssigns))) =>
            checkMergeConditionResolution(target, source, mergeCondition)
            checkMatchedClausesResolution(target, source, None, None, updateAssigns)
            checkNotMatchedClausesResolution(target, source, None, insertAssigns)
            checkNotMatchedBySourceClausesResolution(target, None, None,
              notMatchedBySourceUpdateAssigns)

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
             |WHEN NOT MATCHED BY SOURCE AND (target.s='delete') THEN DELETE
             |WHEN NOT MATCHED BY SOURCE AND (target.s='update') THEN UPDATE SET target.s = 'delete'
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
                  insertAssigns)),
              Seq(DeleteAction(Some(EqualTo(ndl: AttributeReference, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(nul: AttributeReference, StringLiteral("update"))),
                  notMatchedBySourceUpdateAssigns))) =>
            checkMergeConditionResolution(target, source, mergeCondition)
            checkMatchedClausesResolution(target, source, Some(dl), Some(ul), updateAssigns)
            checkNotMatchedClausesResolution(target, source, Some(il), insertAssigns)
            checkNotMatchedBySourceClausesResolution(target, Some(ndl), Some(nul),
              notMatchedBySourceUpdateAssigns)

          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }

        // cte
        val sql5 =
          s"""
             |WITH source(s, i) AS
             | (SELECT * FROM $source)
             |MERGE INTO $target AS target
             |USING source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update') THEN UPDATE SET target.s = source.s
             |WHEN NOT MATCHED AND (source.s='insert')
             |THEN INSERT (target.i, target.s) values (source.i, source.s)
             |WHEN NOT MATCHED BY SOURCE AND (target.s='delete') THEN DELETE
             |WHEN NOT MATCHED BY SOURCE AND (target.s='update') THEN UPDATE SET target.s = 'delete'
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
                  insertAssigns)),
              Seq(DeleteAction(Some(EqualTo(ndl: AttributeReference, StringLiteral("delete")))),
                UpdateAction(Some(EqualTo(nul: AttributeReference, StringLiteral("update"))),
                  notMatchedBySourceUpdateAssigns))) =>
            checkMergeConditionResolution(target, source, mergeCondition)
            checkMatchedClausesResolution(target, source, Some(dl), Some(ul), updateAssigns)
            checkNotMatchedClausesResolution(target, source, Some(il), insertAssigns)
            checkNotMatchedBySourceClausesResolution(target, Some(ndl), Some(nul),
              notMatchedBySourceUpdateAssigns)
          case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }

        // DEFAULT columns (implicit):
        // All cases of the $target table lack any explicitly-defined DEFAULT columns. Therefore any
        // DEFAULT column references in the below MERGE INTO command should resolve to literal NULL.
        // This test case covers that behavior.
        val sql6 =
          s"""
             |MERGE INTO $target AS target
             |USING $source AS source
             |ON target.i = source.i
             |WHEN MATCHED AND (target.s='delete') THEN DELETE
             |WHEN MATCHED AND (target.s='update')
             |THEN UPDATE SET target.s = DEFAULT, target.i = target.i
             |WHEN NOT MATCHED AND (source.s='insert')
             |  THEN INSERT (target.i, target.s) values (DEFAULT, DEFAULT)
             |WHEN NOT MATCHED BY SOURCE AND (target.s='delete') THEN DELETE
             |WHEN NOT MATCHED BY SOURCE AND (target.s='update') THEN UPDATE SET target.s = DEFAULT
           """.stripMargin
        parseAndResolve(sql6) match {
          case m: MergeIntoTable =>
            val source = m.sourceTable
            val target = m.targetTable
            val ti = target.output.find(_.name == "i").get.asInstanceOf[AttributeReference]
            val si = source.output.find(_.name == "i").get.asInstanceOf[AttributeReference]
            m.mergeCondition match {
              case EqualTo(l: AttributeReference, r: AttributeReference) =>
                assert(l.sameRef(ti) && r.sameRef(si))
              case Literal(_, BooleanType) => // this is acceptable as a merge condition
              case other => fail("unexpected merge condition " + other)
            }
            assert(m.matchedActions.length == 2)
            val first = m.matchedActions(0)
            first match {
              case DeleteAction(Some(EqualTo(_: AttributeReference, StringLiteral("delete")))) =>
              case other => fail("unexpected first matched action " + other)
            }
            val second = m.matchedActions(1)
            second match {
              case UpdateAction(Some(EqualTo(_: AttributeReference, StringLiteral("update"))),
                Seq(
                  Assignment(_: AttributeReference, Literal(null, StringType)),
                  Assignment(_: AttributeReference, _: AttributeReference))) =>
              case other => fail("unexpected second matched action " + other)
            }
            assert(m.notMatchedActions.length == 1)
            val negative = m.notMatchedActions(0)
            negative match {
              case InsertAction(Some(EqualTo(_: AttributeReference, StringLiteral("insert"))),
              Seq(Assignment(i: AttributeReference, Literal(null, IntegerType)),
              Assignment(s: AttributeReference, Literal(null, StringType)))) =>
                assert(i.name == "i")
                assert(s.name == "s")
              case other => fail("unexpected not matched action " + other)
            }
            assert(m.notMatchedBySourceActions.length == 2)
            m.notMatchedBySourceActions(0) match {
              case DeleteAction(Some(EqualTo(_: AttributeReference, StringLiteral("delete")))) =>
              case other => fail("unexpected first not matched by source action " + other)
            }
            m.notMatchedBySourceActions(1) match {
              case UpdateAction(Some(EqualTo(_: AttributeReference, StringLiteral("update"))),
                Seq(Assignment(_: AttributeReference, Literal(null, StringType)))) =>
              case other =>
                fail("unexpected second not matched by source action " + other)
            }

          case other =>
            fail("Expect MergeIntoTable, but got:\n" + other.treeString)
        }
    }

    // DEFAULT columns (explicit):
    // The defaultvalues table includes explicitly-defined DEFAULT columns in its schema. Therefore,
    // DEFAULT column references in the below MERGE INTO command should resolve to the corresponding
    // values. This test case covers that behavior.
    val mergeDefaultWithExplicitDefaultColumns =
      """
        |MERGE INTO testcat.defaultvalues AS target
        |USING testcat.tab1 AS source
        |ON target.i = source.i
        |WHEN MATCHED AND (target.s = 31) THEN DELETE
        |WHEN MATCHED AND (target.s = 31)
        |  THEN UPDATE SET target.s = DEFAULT
        |WHEN NOT MATCHED AND (source.s='insert')
        |  THEN INSERT (target.i, target.s) values (DEFAULT, DEFAULT)
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 31) THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 31)
        |  THEN UPDATE SET target.s = DEFAULT
        |""".stripMargin
    parseAndResolve(mergeDefaultWithExplicitDefaultColumns) match {
      case m: MergeIntoTable =>
        val cond = m.mergeCondition
        cond match {
          case EqualTo(Cast(l: AttributeReference, IntegerType, _, _), r: AttributeReference) =>
            assert(l.name == "i")
            assert(r.name == "i")
          case EqualTo(l: AttributeReference, r: AttributeReference) =>
            // ANSI mode on.
            assert(l.name == "i")
            assert(r.name == "i")
          case Literal(_, BooleanType) => // this is acceptable as a merge condition
          case other => fail("unexpected merge condition " + other)
        }
        assert(m.matchedActions.length == 2)
        val first = m.matchedActions(0)
        first match {
          case DeleteAction(Some(EqualTo(_: AttributeReference, Literal(31, IntegerType)))) =>
          case other => fail("unexpected first matched action " + other)
        }
        val second = m.matchedActions(1)
        second match {
          case UpdateAction(Some(EqualTo(_: AttributeReference, Literal(31, IntegerType))),
          Seq(Assignment(_: AttributeReference, Literal(42, IntegerType)))) =>
          case other => fail("unexpected second matched action " + other)
        }
        assert(m.notMatchedActions.length == 1)
        val negative = m.notMatchedActions(0)
        negative match {
          case InsertAction(Some(EqualTo(_: AttributeReference, StringLiteral("insert"))),
          Seq(Assignment(_: AttributeReference, Literal(true, BooleanType)),
          Assignment(_: AttributeReference, Literal(42, IntegerType)))) =>
          case other => fail("unexpected not matched action " + other)
        }
        assert(m.notMatchedBySourceActions.length == 2)
        m.notMatchedBySourceActions(0) match {
          case DeleteAction(Some(EqualTo(_: AttributeReference, Literal(31, IntegerType)))) =>
          case other => fail("unexpected first not matched by source action " + other)
        }
        m.notMatchedBySourceActions(1) match {
          case UpdateAction(Some(EqualTo(_: AttributeReference, Literal(31, IntegerType))),
          Seq(Assignment(_: AttributeReference, Literal(42, IntegerType)))) =>
          case other => fail("unexpected second not matched by source action " + other)
        }

      case other =>
        fail("Expect MergeIntoTable, but got:\n" + other.treeString)
    }

    // DEFAULT column reference in the merge condition:
    // This MERGE INTO command includes an ON clause with a DEFAULT column reference. This
    // DEFAULT column won't be resolved.
    val mergeWithDefaultReferenceInMergeCondition =
      """
        |MERGE INTO testcat.tab AS target
        |USING testcat.tab1 AS source
        |ON target.i = DEFAULT
        |WHEN MATCHED AND (target.s = 31) THEN DELETE
        |WHEN MATCHED AND (target.s = 31)
        |  THEN UPDATE SET target.s = DEFAULT
        |WHEN NOT MATCHED AND (source.s='insert')
        |  THEN INSERT (target.i, target.s) values (DEFAULT, DEFAULT)
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 31) THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 31)
        |  THEN UPDATE SET target.s = DEFAULT
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException] {
        parseAndResolve(mergeWithDefaultReferenceInMergeCondition, checkAnalysis = true)
      },
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`DEFAULT`",
        "proposal" -> "`target`.`i`, `source`.`i`, `target`.`s`, `source`.`s`"),
      context = ExpectedContext(
        fragment = "DEFAULT",
        start = 77,
        stop = 83))

    // DEFAULT column reference within a complex expression:
    // This MERGE INTO command includes a WHEN MATCHED clause with a DEFAULT column reference as
    // of a complex expression (DEFAULT + 1). This is invalid and column won't be resolved.
    val mergeWithDefaultReferenceAsPartOfComplexExpression =
      """
        |MERGE INTO testcat.tab AS target
        |USING testcat.tab1 AS source
        |ON target.i = source.i
        |WHEN MATCHED AND (target.s = 31) THEN DELETE
        |WHEN MATCHED AND (target.s = 31)
        |  THEN UPDATE SET target.s = DEFAULT + 1
        |WHEN NOT MATCHED AND (source.s='insert')
        |  THEN INSERT (target.i, target.s) values (DEFAULT, DEFAULT)
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 31) THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 31)
        |  THEN UPDATE SET target.s = DEFAULT + 1
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException] {
        parseAndResolve(mergeWithDefaultReferenceAsPartOfComplexExpression)
      },
      errorClass = "_LEGACY_ERROR_TEMP_1343",
      parameters = Map.empty)

    val mergeWithDefaultReferenceForNonNullableCol =
      """
        |MERGE INTO testcat.tab2 AS target
        |USING testcat.tab1 AS source
        |ON target.i = source.i
        |WHEN NOT MATCHED AND (source.s = 'insert')
        |  THEN INSERT (target.i, target.x) VALUES (1, DEFAULT)
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException] {
        parseAndResolve(mergeWithDefaultReferenceForNonNullableCol)
      },
      errorClass = "NO_DEFAULT_COLUMN_VALUE_AVAILABLE",
      parameters = Map("colName" -> "`x`")
    )

    // Ambiguous DEFAULT column reference when the table itself contains a column named
    // "DEFAULT".
    val mergeIntoTableWithColumnNamedDefault =
      """
        |MERGE INTO testcat.tablewithcolumnnameddefault AS target
        |USING testcat.tab1 AS source
        |ON default = source.i
        |WHEN MATCHED AND (target.s = 32) THEN DELETE
        |WHEN MATCHED AND (target.s = 32)
        |  THEN UPDATE SET target.s = DEFAULT
        |WHEN NOT MATCHED AND (source.s='insert')
        |  THEN INSERT (target.s) values (DEFAULT)
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 32) THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.s = 32)
        |  THEN UPDATE SET target.s = DEFAULT
        |""".stripMargin
    parseAndResolve(mergeIntoTableWithColumnNamedDefault, withDefault = true) match {
      case m: MergeIntoTable =>
        val target = m.targetTable
        val d = target.output.find(_.name == "default").get.asInstanceOf[AttributeReference]
        m.mergeCondition match {
          case EqualTo(Cast(l: AttributeReference, _, _, _), _) =>
            assert(l.sameRef(d))
          case Literal(_, BooleanType) => // this is acceptable as a merge condition
          case other =>
            fail("unexpected merge condition " + other)
        }
        assert(m.matchedActions.length == 2)
        assert(m.notMatchedActions.length == 1)
        assert(m.notMatchedBySourceActions.length == 2)

      case other =>
        fail("Expect MergeIntoTable, but got:\n" + other.treeString)
    }

    // no aliases
    Seq(("v2Table", "v2Table1"), ("testcat.tab", "testcat.tab1")).foreach { pair =>
      def referenceNames(target: String, column: String): String = target match {
        case "v2Table" => s"[`spark_catalog`.`default`.`v2Table1`.`$column`, " +
          s"`spark_catalog`.`default`.`v2Table`.`$column`]"
        case "testcat.tab" => s"[`testcat`.`tab1`.`$column`, `testcat`.`tab`.`$column`]"
      }

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
           |WHEN NOT MATCHED BY SOURCE AND (${target}.s='delete') THEN DELETE
           |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET s = 1
         """.stripMargin

      parseAndResolve(sql1) match {
        case MergeIntoTable(
            AsDataSourceV2Relation(target),
            AsDataSourceV2Relation(source),
            _,
            Seq(DeleteAction(Some(_)), UpdateAction(None, firstUpdateAssigns)),
            Seq(InsertAction(
              Some(EqualTo(il: AttributeReference, StringLiteral("a"))),
            insertAssigns)),
            Seq(DeleteAction(Some(_)), UpdateAction(None, secondUpdateAssigns))) =>
          val ti = target.output.find(_.name == "i").get
          val ts = target.output.find(_.name == "s").get
          val si = source.output.find(_.name == "i").get
          val ss = source.output.find(_.name == "s").get

          // INSERT condition is resolved with source table only, so column `s` is not ambiguous.
          assert(il.sameRef(ss))
          assert(firstUpdateAssigns.size == 1)
          // UPDATE key is resolved with target table only, so column `s` is not ambiguous.
          assert(firstUpdateAssigns.head.key.asInstanceOf[AttributeReference].sameRef(ts))
          assert(insertAssigns.size == 1)
          // INSERT key is resolved with target table only, so column `i` is not ambiguous.
          assert(insertAssigns.head.key.asInstanceOf[AttributeReference].sameRef(ti))
          // INSERT value is resolved with source table only, so column `i` is not ambiguous.
          assert(insertAssigns.head.value.asInstanceOf[AttributeReference].sameRef(si))
          assert(secondUpdateAssigns.size == 1)
          // UPDATE key is resolved with target table only, so column `s` is not ambiguous.
          assert(secondUpdateAssigns.head.key.asInstanceOf[AttributeReference].sameRef(ts))

        case p => fail("Expect MergeIntoTable, but got:\n" + p.treeString)
      }

      val sql2 =
        s"""MERGE INTO $target
           |USING $source
           |ON i = 1
           |WHEN MATCHED THEN DELETE""".stripMargin
      // merge condition is resolved with both target and source tables, and we can't
      // resolve column `i` as it's ambiguous.
      checkError(
        exception = intercept[AnalysisException](parseAndResolve(sql2)),
        errorClass = "AMBIGUOUS_REFERENCE",
        parameters = Map("name" -> "`i`", "referenceNames" -> referenceNames(target, "i")),
        context = ExpectedContext(
          fragment = "i",
          start = 22 + target.length + source.length,
          stop = 22 + target.length + source.length))

      val sql3 =
        s"""MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN MATCHED AND (s='delete') THEN DELETE""".stripMargin
      // delete condition is resolved with both target and source tables, and we can't
      // resolve column `s` as it's ambiguous.
      checkError(
        exception = intercept[AnalysisException](parseAndResolve(sql3)),
        errorClass = "AMBIGUOUS_REFERENCE",
        parameters = Map("name" -> "`s`", "referenceNames" -> referenceNames(target, "s")),
        context = ExpectedContext(
          fragment = "s",
          start = 46 + target.length + source.length,
          stop = 46 + target.length + source.length))

      val sql4 =
        s"""MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN MATCHED AND (s = 'a') THEN UPDATE SET i = 1""".stripMargin
      // update condition is resolved with both target and source tables, and we can't
      // resolve column `s` as it's ambiguous.
      checkError(
        exception = intercept[AnalysisException](parseAndResolve(sql4)),
        errorClass = "AMBIGUOUS_REFERENCE",
        parameters = Map("name" -> "`s`", "referenceNames" -> referenceNames(target, "s")),
        context = ExpectedContext(
          fragment = "s",
          start = 46 + target.length + source.length,
          stop = 46 + target.length + source.length))

      val sql5 =
        s"""MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN MATCHED THEN UPDATE SET s = s""".stripMargin
      // update value is resolved with both target and source tables, and we can't
      // resolve column `s` as it's ambiguous.
      checkError(
        exception = intercept[AnalysisException](parseAndResolve(sql5)),
        errorClass = "AMBIGUOUS_REFERENCE",
        parameters = Map("name" -> "`s`", "referenceNames" -> referenceNames(target, "s")),
        context = ExpectedContext(
          fragment = "s",
          start = 61 + target.length + source.length,
          stop = 61 + target.length + source.length))

      val sql6 =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON target.i = source.i
           |WHEN NOT MATCHED BY SOURCE AND (s = 'b') THEN DELETE
           |WHEN NOT MATCHED BY SOURCE AND (s = 'a') THEN UPDATE SET i = 1
         """.stripMargin
      // not matched by source clauses are resolved using the target table only, resolving columns
      // is not ambiguous.
      val parsed = parseAndResolve(sql6)
      parsed match {
        case MergeIntoTable(
            AsDataSourceV2Relation(target),
            _,
            _,
            Seq(),
            Seq(),
            notMatchedBySourceActions) =>
          assert(notMatchedBySourceActions.length == 2)
          notMatchedBySourceActions(0) match {
            case DeleteAction(Some(EqualTo(dl: AttributeReference, StringLiteral("b")))) =>
              // DELETE condition is resolved with target table only, so column `s` is not
              // ambiguous.
              val ts = target.output.find(_.name == "s").get
              assert(dl.sameRef(ts))
            case other => fail("unexpected first not matched by source action " + other)
          }
          notMatchedBySourceActions(1) match {
            case UpdateAction(Some(EqualTo(ul: AttributeReference, StringLiteral("a"))),
                Seq(Assignment(us: AttributeReference, IntegerLiteral(1)))) =>
              // UPDATE condition and assignment are resolved with target table only, so column `s`
              // and `i` are not ambiguous.
              val ts = target.output.find(_.name == "s").get
              val ti = target.output.find(_.name == "i").get
              assert(ul.sameRef(ts))
              assert(us.sameRef(ti))
            case other => fail("unexpected second not matched by source action " + other)
          }
      }

      val sql7 =
        s"""
           |MERGE INTO $target
           |USING $source
           |ON 1 = 1
           |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET $target.s = $source.s
         """.stripMargin
      // update value in not matched by source clause can only reference the target table.
      checkError(
        exception = intercept[AnalysisException](parseAndResolve(sql7)),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map("objectName" -> s"${toSQLId(source)}.`s`", "proposal" -> "`i`, `s`"),
        context = ExpectedContext(
          fragment = s"$source.s",
          start = 77 + target.length * 2 + source.length,
          stop = 78 + target.length * 2 + source.length * 2))
    }

    val sql1 =
      s"""
         |MERGE INTO non_exist_target
         |USING non_exist_source
         |ON target.i = source.i
         |WHEN MATCHED AND (non_exist_target.s='delete') THEN DELETE
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
       """.stripMargin
    val parsed = parseAndResolve(sql1)
    parsed match {
      case u: MergeIntoTable =>
        assert(u.targetTable.isInstanceOf[UnresolvedRelation])
        assert(u.sourceTable.isInstanceOf[UnresolvedRelation])
      case _ => fail("Expect MergeIntoTable, but got:\n" + parsed.treeString)
    }

    // UPDATE * with incompatible schema between source and target tables.
    val sql2 =
      """MERGE INTO testcat.tab
         |USING testcat.tab2
         |ON 1 = 1
         |WHEN MATCHED THEN UPDATE SET *""".stripMargin
    checkError(
      exception = intercept[AnalysisException](parseAndResolve(sql2)),
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`s`", "proposal" -> "`i`, `x`"),
      context = ExpectedContext(fragment = sql2, start = 0, stop = 80))

    // INSERT * with incompatible schema between source and target tables.
    val sql3 =
      """MERGE INTO testcat.tab
        |USING testcat.tab2
        |ON 1 = 1
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin
    checkError(
      exception = intercept[AnalysisException](parseAndResolve(sql3)),
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`s`", "proposal" -> "`i`, `x`"),
      context = ExpectedContext(fragment = sql3, start = 0, stop = 80))

    val sql4 =
      """
        |MERGE INTO testcat.charvarchar
        |USING testcat.tab2
        |ON 1 = 1
        |WHEN MATCHED THEN UPDATE SET c1='a', c2=1
        |WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES ('b', 2)
        |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET c1='a', c2=1
        |""".stripMargin
    val parsed4 = parseAndResolve(sql4)
    parsed4 match {
      case m: MergeIntoTable =>
        assert(m.matchedActions.length == 1)
        m.matchedActions.head match {
          case UpdateAction(_, Seq(
          Assignment(_, s1: StaticInvoke), Assignment(_, s2: StaticInvoke))) =>
            assert(s1.arguments.length == 2)
            assert(s1.functionName == "charTypeWriteSideCheck")
            assert(s2.arguments.length == 2)
            assert(s2.arguments.head.isInstanceOf[Cast])
            val cast = s2.arguments.head.asInstanceOf[Cast]
            assert(cast.getTagValue(Cast.BY_TABLE_INSERTION).isDefined)
            assert(s2.functionName == "varcharTypeWriteSideCheck")
          case other => fail("Expect UpdateAction, but got: " + other)
        }
        assert(m.notMatchedActions.length == 1)
        m.notMatchedActions.head match {
          case InsertAction(_, Seq(
          Assignment(_, s1: StaticInvoke), Assignment(_, s2: StaticInvoke))) =>
            assert(s1.arguments.length == 2)
            assert(s1.functionName == "charTypeWriteSideCheck")
            assert(s2.arguments.length == 2)
            assert(s2.arguments.head.isInstanceOf[Cast])
            val cast = s2.arguments.head.asInstanceOf[Cast]
            assert(cast.getTagValue(Cast.BY_TABLE_INSERTION).isDefined)
            assert(s2.functionName == "varcharTypeWriteSideCheck")
          case other => fail("Expect UpdateAction, but got: " + other)
        }
        assert(m.notMatchedBySourceActions.length == 1)
        m.notMatchedBySourceActions.head match {
          case UpdateAction(_, Seq(
          Assignment(_, s1: StaticInvoke), Assignment(_, s2: StaticInvoke))) =>
            assert(s1.arguments.length == 2)
            assert(s1.functionName == "charTypeWriteSideCheck")
            assert(s2.arguments.length == 2)
            assert(s2.arguments.head.isInstanceOf[Cast])
            val cast = s2.arguments.head.asInstanceOf[Cast]
            assert(cast.getTagValue(Cast.BY_TABLE_INSERTION).isDefined)
            assert(s2.functionName == "varcharTypeWriteSideCheck")
          case other => fail("Expect UpdateAction, but got: " + other)
        }
      case other => fail("Expect MergeIntoTable, but got:\n" + other.treeString)
    }
  }

  test("MERGE INTO TABLE - skip filling missing cols on v2 tables that accept any schema") {
    val sql =
      s"""
         |MERGE INTO testcat.v2TableWithAcceptAnySchemaCapability AS target
         |USING v2Table AS source
         |ON target.i = source.i
         |WHEN MATCHED THEN DELETE
         |WHEN NOT MATCHED THEN INSERT (target.i) values (DEFAULT)
       """.stripMargin

    parseAndResolve(sql) match {
      case MergeIntoTable(
          SubqueryAlias(AliasIdentifier("target", Seq()), AsDataSourceV2Relation(_)),
          SubqueryAlias(AliasIdentifier("source", Seq()), AsDataSourceV2Relation(_)),
          _,
          Seq(DeleteAction(None)),
          Seq(InsertAction(None, insertAssigns)),
          Nil) =>
        // There is only one assignment, the missing col is not filled with default value
        assert(insertAssigns.size == 1)
        // Special case: Spark does not resolve any columns in MERGE if table accepts any schema.
        assert(insertAssigns.head.key.asInstanceOf[UnresolvedAttribute].name == "target.i")
        assert(insertAssigns.head.value.asInstanceOf[UnresolvedAttribute].name == "DEFAULT")

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
        case CreateTableV1(tableDesc, mode, query) =>
          val newTableDesc = tableDesc.copy(createTime = -1L)
          CreateTableV1(newTableDesc, mode, query)
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
        query: Option[LogicalPlan] = None): CreateTableV1 = {
      CreateTableV1(
        CatalogTable(
          identifier = TableIdentifier(table, database,
            if (database.isDefined) Some(SESSION_CATALOG_NAME) else None),
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

    val sql = "CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING)"
    checkError(
      exception = parseException(parsePlan)(sql),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "':'", "hint" -> ""))
  }

  test("create hive table - table file format") {
    val allSources = Seq("parquet", "parquetfile", "orc", "orcfile", "avro", "avrofile",
      "sequencefile", "rcfile", "textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab STORED AS $s"
      parseAndResolve(query) match {
        case ct: CreateTableV1 =>
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
      case parsed1: CreateTableV1 =>
        assert(parsed1.tableDesc.storage.serde == Some("anything"))
        assert(parsed1.tableDesc.storage.inputFormat == Some("inputfmt"))
        assert(parsed1.tableDesc.storage.outputFormat == Some("outputfmt"))
    }

    parseAndResolve(query2) match {
      case parsed2: CreateTableV1 =>
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
          case ct: CreateTableV1 =>
            val hiveSerde = HiveSerDe.sourceToSerDe(s)
            assert(hiveSerde.isDefined)
            assert(ct.tableDesc.storage.serde == Some("anything"))
            assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
            assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
        }
      } else {
        assertUnsupported(
          query,
          Map("message" -> (s"ROW FORMAT SERDE is incompatible with format '$s', " +
            "which also specifies a serde")),
          ExpectedContext(fragment = query, start = 0, stop = 57 + s.length))
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
          case ct: CreateTableV1 =>
            val hiveSerde = HiveSerDe.sourceToSerDe(s)
            assert(hiveSerde.isDefined)
            assert(ct.tableDesc.storage.serde == hiveSerde.get.serde
                .orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
            assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
            assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
        }
      } else {
        assertUnsupported(
          query,
          Map("message" -> ("ROW FORMAT DELIMITED is only compatible with 'textfile', " +
            s"not '$s'")),
          ExpectedContext(fragment = query, start = 0, stop = 75 + s.length))
      }
    }
  }

  test("create hive external table") {
    val withoutLoc = "CREATE EXTERNAL TABLE my_tab STORED AS parquet"
    parseAndResolve(withoutLoc) match {
      case ct: CreateTableV1 =>
        assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
        assert(ct.tableDesc.storage.locationUri.isEmpty)
    }

    val withLoc = "CREATE EXTERNAL TABLE my_tab STORED AS parquet LOCATION '/something/anything'"
    parseAndResolve(withLoc) match {
      case ct: CreateTableV1 =>
        assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
        assert(ct.tableDesc.storage.locationUri == Some(new URI("/something/anything")))
    }
  }

  test("create hive table - property values must be set") {
    val sql1 = "CREATE TABLE my_tab STORED AS parquet " +
      "TBLPROPERTIES('key_without_value', 'key_with_value'='x')"
    assertUnsupported(
      sql1,
      Map("message" -> "Values must be specified for key(s): [key_without_value]"),
      ExpectedContext(fragment = sql1, start = 0, stop = 93))

    val sql2 = "CREATE TABLE my_tab ROW FORMAT SERDE 'serde' " +
      "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')"
    assertUnsupported(
      sql2,
      Map("message" -> "Values must be specified for key(s): [key_without_value]"),
      ExpectedContext(
        fragment = "ROW FORMAT SERDE 'serde' WITH SERDEPROPERTIES('key_without_value', " +
          "'key_with_value'='x')",
        start = 20,
        stop = 107))
  }

  test("create hive table - location implies external") {
    val query = "CREATE TABLE my_tab STORED AS parquet LOCATION '/something/anything'"
    parseAndResolve(query) match {
      case ct: CreateTableV1 =>
        assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
        assert(ct.tableDesc.storage.locationUri == Some(new URI("/something/anything")))
    }
  }

  test("Duplicate clauses - create hive table") {
    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) STORED AS parquet $duplicateClause $duplicateClause"
    }

    val sql1 = createTableHeader("TBLPROPERTIES('test' = 'test2')")
    checkError(
      exception = parseException(parsePlan)(sql1),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "TBLPROPERTIES"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 117))

    val sql2 = createTableHeader("LOCATION '/tmp/file'")
    checkError(
      exception = parseException(parsePlan)(sql2),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "LOCATION"),
      context = ExpectedContext(fragment = sql2, start = 0, stop = 95))

    val sql3 = createTableHeader("COMMENT 'a table'")
    checkError(
      exception = parseException(parsePlan)(sql3),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "COMMENT"),
      context = ExpectedContext(fragment = sql3, start = 0, stop = 89))

    val sql4 = createTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS")
    checkError(
      exception = parseException(parsePlan)(sql4),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "CLUSTERED BY"),
      context = ExpectedContext(fragment = sql4, start = 0, stop = 119))

    val sql5 = createTableHeader("PARTITIONED BY (k int)")
    checkError(
      exception = parseException(parsePlan)(sql5),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "PARTITIONED BY"),
      context = ExpectedContext(fragment = sql5, start = 0, stop = 99))

    val sql6 = createTableHeader("STORED AS parquet")
    checkError(
      exception = parseException(parsePlan)(sql6),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "STORED AS/BY"),
      context = ExpectedContext(fragment = sql6, start = 0, stop = 89))

    val sql7 = createTableHeader("ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'")
    checkError(
      exception = parseException(parsePlan)(sql7),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "ROW FORMAT"),
      context = ExpectedContext(fragment = sql7, start = 0, stop = 163))
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
    checkError(
      exception = intercept[ParseException] {
        extractTableDesc(s4)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "STORED BY"),
      context = ExpectedContext(
        fragment = "STORED BY 'storage.handler.class.name'",
        start = 23,
        stop = 60))
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
    val sql = s"CREATE TABLE ctas1 PARTITIONED BY (k int)" +
      " AS SELECT key, value FROM (SELECT 1 as key, 2 as value) tmp"
    assertUnsupported(
      sql,
      Map("message" ->
        "Partition column types may not be specified in Create Table As Select (CTAS)"),
      ExpectedContext(fragment = sql, start = 0, stop = 100))
  }

  test("CTAS statement with schema") {
    val sql1 = s"CREATE TABLE ctas1 (age INT, name STRING) AS SELECT * FROM src"
    assertUnsupported(
      sql1,
      Map("message" -> "Schema may not be specified in a Create Table As Select (CTAS) statement"),
      ExpectedContext(fragment = sql1, start = 0, stop = 61))

    val sql2 = s"CREATE TABLE ctas1 (age INT, name STRING) AS SELECT 1, 'hello'"
    assertUnsupported(
      sql2,
      Map("message" -> "Schema may not be specified in a Create Table As Select (CTAS) statement"),
      ExpectedContext(fragment = sql2, start = 0, stop = 61))
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
    checkError(
      exception = intercept[ParseException] {
        parsePlan(query)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map(
        "message" -> "CREATE TEMPORARY TABLE ..., use CREATE TEMPORARY VIEW instead"),
      context = ExpectedContext(fragment = query, start = 0, stop = 48))
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
    checkError(
      exception = intercept[ParseException] {
        parsePlan(query1)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "CREATE TABLE ... SKEWED BY"),
      context = ExpectedContext(fragment = query1, start = 0, stop = 72))

    val query2 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z'))"
    checkError(
      exception = intercept[ParseException] {
        parsePlan(query2)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "CREATE TABLE ... SKEWED BY"),
      context = ExpectedContext(fragment = query2, start = 0, stop = 96))

    val query3 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z')) STORED AS DIRECTORIES"
    checkError(
      exception = intercept[ParseException] {
        parsePlan(query3)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "CREATE TABLE ... SKEWED BY"),
      context = ExpectedContext(fragment = query3, start = 0, stop = 118))
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
    checkError(
      exception = intercept[ParseException] {
        parsePlan(query1)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "STORED BY"),
      context = ExpectedContext(
        fragment = "STORED BY 'org.papachi.StorageHandler'",
        start = 44,
        stop = 81))

    val query2 = s"$baseQuery 'org.mamachi.StorageHandler' WITH SERDEPROPERTIES ('k1'='v1')"
    checkError(
      exception = intercept[ParseException] {
        parsePlan(query2)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "STORED BY"),
      context = ExpectedContext(
        fragment = "STORED BY 'org.mamachi.StorageHandler' WITH SERDEPROPERTIES ('k1'='v1')",
        start = 44,
        stop = 114))
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

  test("SPARK-34701: children/innerChildren should be mutually exclusive for AnalysisOnlyCommand") {
    val cmdNotAnalyzed = DummyAnalysisOnlyCommand(isAnalyzed = false, childrenToAnalyze = Seq(null))
    assert(cmdNotAnalyzed.innerChildren.isEmpty)
    assert(cmdNotAnalyzed.children.length == 1)
    val cmdAnalyzed = cmdNotAnalyzed.markAsAnalyzed(AnalysisContext.get)
    assert(cmdAnalyzed.innerChildren.length == 1)
    assert(cmdAnalyzed.children.isEmpty)
  }

  // TODO: add tests for more commands.
}

object AsDataSourceV2Relation {
  def unapply(plan: LogicalPlan): Option[DataSourceV2Relation] = plan match {
    case SubqueryAlias(_, r: DataSourceV2Relation) => Some(r)
    case _ => None
  }
}

case class DummyAnalysisOnlyCommand(
    isAnalyzed: Boolean,
    childrenToAnalyze: Seq[LogicalPlan]) extends AnalysisOnlyCommand {
  override def markAsAnalyzed(ac: AnalysisContext): LogicalPlan = copy(isAnalyzed = true)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    copy(childrenToAnalyze = newChildren)
  }
}
