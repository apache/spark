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

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin, Identifier, LookupCatalog, TableCatalog, TestTableCatalog}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateV2Table, DropTable, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSourceResolution}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.internal.SQLConf.DEFAULT_V2_CATALOG
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PlanResolutionSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val orc2 = classOf[OrcDataSourceV2].getName

  private val testCat: TableCatalog = {
    val newCatalog = new TestTableCatalog
    newCatalog.initialize("testcat", CaseInsensitiveStringMap.empty())
    newCatalog
  }

  private val v2SessionCatalog = {
    val newCatalog = new TestTableCatalog
    newCatalog.initialize("session", CaseInsensitiveStringMap.empty())
    newCatalog
  }

  private val lookupWithDefault: LookupCatalog = new LookupCatalog {
    override protected def defaultCatalogName: Option[String] = Some("testcat")

    override protected def lookupCatalog(name: String): CatalogPlugin = name match {
      case "testcat" =>
        testCat
      case "session" =>
        v2SessionCatalog
      case _ =>
        throw new CatalogNotFoundException(s"No such catalog: $name")
    }
  }

  private val lookupWithoutDefault: LookupCatalog = new LookupCatalog {
    override protected def defaultCatalogName: Option[String] = None

    override protected def lookupCatalog(name: String): CatalogPlugin = name match {
      case "testcat" =>
        testCat
      case "session" =>
        v2SessionCatalog
      case _ =>
        throw new CatalogNotFoundException(s"No such catalog: $name")
    }
  }

  def parseAndResolve(query: String, withDefault: Boolean = false): LogicalPlan = {
    val newConf = conf.copy()
    newConf.setConfString(DEFAULT_V2_CATALOG.key, "testcat")
    DataSourceResolution(newConf, if (withDefault) lookupWithDefault else lookupWithoutDefault)
        .apply(parsePlan(query))
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
      assert(desc.viewDefaultDatabase.isEmpty)
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
         |USING $orc2
         |COMMENT 'This is the staging page view table'
         |LOCATION '/user/external/page_view'
         |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "provider" -> orc2,
      "location" -> "/user/external/page_view",
      "comment" -> "This is the staging page view table")

    parseAndResolve(sql) match {
      case create: CreateV2Table =>
        assert(create.catalog.name == "session")
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
        |USING $orc2
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val expectedProperties = Map(
      "p1" -> "v1",
      "p2" -> "v2",
      "provider" -> orc2,
      "location" -> "/user/external/page_view",
      "comment" -> "This is the staging page view table")

    parseAndResolve(sql) match {
      case ctas: CreateTableAsSelect =>
        assert(ctas.catalog.name == "session")
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
    val sql1_table = "ALTER TABLE table_name SET TBLPROPERTIES ('test' = 'test', " +
        "'comment' = 'new_comment')"
    val sql2_table = "ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_table = "ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

    val parsed1_table = parseAndResolve(sql1_table)
    val parsed2_table = parseAndResolve(sql2_table)
    val parsed3_table = parseAndResolve(sql3_table)

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_table = AlterTableSetPropertiesCommand(
      tableIdent, Map("test" -> "test", "comment" -> "new_comment"), isView = false)
    val expected2_table = AlterTableUnsetPropertiesCommand(
      tableIdent, Seq("comment", "test"), ifExists = false, isView = false)
    val expected3_table = AlterTableUnsetPropertiesCommand(
      tableIdent, Seq("comment", "test"), ifExists = true, isView = false)

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
    comparePlans(parsed3_table, expected3_table)
  }

  test("support for other types in TBLPROPERTIES") {
    val sql =
      """
        |ALTER TABLE table_name
        |SET TBLPROPERTIES ('a' = 1, 'b' = 0.1, 'c' = TRUE)
      """.stripMargin
    val parsed = parseAndResolve(sql)
    val expected = AlterTableSetPropertiesCommand(
      TableIdentifier("table_name"),
      Map("a" -> "1", "b" -> "0.1", "c" -> "true"),
      isView = false)

    comparePlans(parsed, expected)
  }

  test("alter table: set location") {
    val sql1 = "ALTER TABLE table_name SET LOCATION 'new location'"
    val parsed1 = parseAndResolve(sql1)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSetLocationCommand(
      tableIdent,
      None,
      "new location")
    comparePlans(parsed1, expected1)
  }
}
