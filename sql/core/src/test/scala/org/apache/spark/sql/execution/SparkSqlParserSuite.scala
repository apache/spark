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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{DescribeFunctionCommand, DescribeTableCommand,
  ShowFunctionsCommand}
import org.apache.spark.sql.execution.datasources.{CreateTable, CreateTempViewUsing}
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * Parser test cases for rules defined in [[SparkSqlParser]].
 *
 * See [[org.apache.spark.sql.catalyst.parser.PlanParserSuite]] for rules
 * defined in the Catalyst module.
 */
class SparkSqlParserSuite extends PlanTest {

  private lazy val parser = new SparkSqlParser(new SQLConf)

  /**
   * Normalizes plans:
   * - CreateTable the createTime in tableDesc will replaced by -1L.
   */
  private def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case CreateTable(tableDesc, mode, query) =>
        val newTableDesc = tableDesc.copy(createTime = -1L)
        CreateTable(newTableDesc, mode, query)
      case _ => plan // Don't transform
    }
  }

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    val normalized1 = normalizePlan(parser.parsePlan(sqlCommand))
    val normalized2 = normalizePlan(plan)
    comparePlans(normalized1, normalized2)
  }

  private def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](parser.parsePlan(sqlCommand))
    messages.foreach { message =>
      assert(e.message.contains(message))
    }
  }

  test("show functions") {
    assertEqual("show functions", ShowFunctionsCommand(None, None, true, true))
    assertEqual("show all functions", ShowFunctionsCommand(None, None, true, true))
    assertEqual("show user functions", ShowFunctionsCommand(None, None, true, false))
    assertEqual("show system functions", ShowFunctionsCommand(None, None, false, true))
    intercept("show special functions", "SHOW special FUNCTIONS")
    assertEqual("show functions foo",
      ShowFunctionsCommand(None, Some("foo"), true, true))
    assertEqual("show functions foo.bar",
      ShowFunctionsCommand(Some("foo"), Some("bar"), true, true))
    assertEqual("show functions 'foo\\\\.*'",
      ShowFunctionsCommand(None, Some("foo\\.*"), true, true))
    intercept("show functions foo.bar.baz", "Unsupported function name")
  }

  test("describe function") {
    assertEqual("describe function bar",
      DescribeFunctionCommand(FunctionIdentifier("bar", database = None), isExtended = false))
    assertEqual("describe function extended bar",
      DescribeFunctionCommand(FunctionIdentifier("bar", database = None), isExtended = true))
    assertEqual("describe function foo.bar",
      DescribeFunctionCommand(
        FunctionIdentifier("bar", database = Some("foo")), isExtended = false))
    assertEqual("describe function extended f.bar",
      DescribeFunctionCommand(FunctionIdentifier("bar", database = Some("f")), isExtended = true))
  }

  private def createTableUsing(
      table: String,
      database: Option[String] = None,
      tableType: CatalogTableType = CatalogTableType.MANAGED,
      storage: CatalogStorageFormat = CatalogStorageFormat.empty,
      schema: StructType = new StructType,
      provider: Option[String] = Some("parquet"),
      partitionColumnNames: Seq[String] = Seq.empty,
      bucketSpec: Option[BucketSpec] = None,
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
        bucketSpec = bucketSpec
      ), mode, query
    )
  }

  private def createTempViewUsing(
      table: String,
      database: Option[String] = None,
      schema: Option[StructType] = None,
      replace: Boolean = true,
      provider: String = "parquet",
      options: Map[String, String] = Map.empty): LogicalPlan = {
    CreateTempViewUsing(TableIdentifier(table, database), schema, replace, provider, options)
  }

  private def createTable(
      table: String,
      database: Option[String] = None,
      tableType: CatalogTableType = CatalogTableType.MANAGED,
      storage: CatalogStorageFormat = CatalogStorageFormat.empty.copy(
        inputFormat = HiveSerDe.sourceToSerDe("textfile").get.inputFormat,
        outputFormat = HiveSerDe.sourceToSerDe("textfile").get.outputFormat),
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

  test("create table using - createTableHeader") {
    assertEqual("CREATE TABLE my_tab USING parquet", createTableUsing(table = "my_tab"))
    assertEqual("CREATE TABLE db.my_tab USING parquet",
      createTableUsing(table = "my_tab", database = Some("db")))
    intercept("CREATE TABLE dup.db.my_tab USING parquet", "mismatched input '.'")
    assertEqual("CREATE TEMPORARY TABLE my_tab USING parquet",
      createTempViewUsing(table = "my_tab"))
    intercept("CREATE EXTERNAL TABLE my_tab USING parquet",
      "Operation not allowed: CREATE EXTERNAL TABLE ... USING")
    assertEqual("CREATE TABLE IF NOT EXISTS my_tab USING parquet",
      createTableUsing(table = "my_tab", mode = SaveMode.Ignore))
    intercept("CREATE TEMPORARY TABLE IF NOT EXISTS my_tab USING parquet",
      "Operation not allowed: CREATE TEMPORARY TABLE ... IF NOT EXISTS")
  }

  test("create table using - schema") {
    assertEqual("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) USING parquet",
      createTableUsing(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType, nullable = true, "test")
          .add("b", StringType)
      )
    )
    intercept("CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING) USING parquet",
      "no viable alternative at input")
  }

  test("create table using - tableProvider") {
    assertEqual("CREATE TABLE my_tab USING json",
      createTableUsing(table = "my_tab", provider = Some("json")))
    val e = intercept[AnalysisException](parser.parsePlan("CREATE TABLE my_tab USING hive"))
    assert(e.message.contains("Cannot create hive serde table"))
  }

  test("create table using - tablePropertyList") {
    assertEqual("CREATE TABLE my_tab USING parquet OPTIONS (a 1, b 0.1, c TRUE)",
      createTableUsing(
        table = "my_tab",
        storage = CatalogStorageFormat.empty.copy(
          properties = Map("a" -> "1", "b" -> "0.1", "c" -> "true")
        )
      )
    )
    assertEqual("CREATE TABLE my_tab USING parquet OPTIONS ('a' = 1, 'b' = 0.1, 'c' = TRUE)",
      createTableUsing(
        table = "my_tab",
        storage = CatalogStorageFormat.empty.copy(
          properties = Map("a" -> "1", "b" -> "0.1", "c" -> "true")
        )
      )
    )
    intercept("CREATE TABLE my_tab USING parquet OPTIONS ()", "no viable alternative at input")
    intercept("CREATE TABLE my_tab USING parquet OPTIONS (a = 1, b)",
      "Operation not allowed: Values must be specified for key(s)")
  }

  test("create table using - partitioned") {
    assertEqual("CREATE TABLE my_tab(a INT, b STRING) USING parquet PARTITIONED BY (a)",
      createTableUsing(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType)
          .add("b", StringType),
        partitionColumnNames = Seq("a")
      )
    )
  }

  test("create table using - bucketSpec") {
    assertEqual("CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
      "CLUSTERED BY (a) INTO 5 BUCKETS",
      createTableUsing(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType)
          .add("b", StringType),
        bucketSpec = Some(BucketSpec(5, Seq("a"), Seq.empty))
      )
    )
    assertEqual("CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
      "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS",
      createTableUsing(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType)
          .add("b", StringType),
        bucketSpec = Some(BucketSpec(5, Seq("a"), Seq("b")))
      )
    )
    intercept("CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
      "CLUSTERED BY (a) SORTED BY (b) INTO 0 BUCKETS", "Expected positive number of buckets")
  }

  test("create table using - CTAS") {
    val query = parser.parsePlan("SELECT a, b FROM t1")
    assertEqual("CREATE TABLE my_tab USING parquet SELECT a, b FROM t1",
      createTableUsing(table = "my_tab", query = Some(query)))
    intercept("CREATE TEMPORARY TABLE my_tab USING parquet AS SELECT a, b FROM t1",
      "Operation not allowed: CREATE TEMPORARY TABLE ... USING ... AS query")
    intercept("CREATE TABLE my_tab(a INT, b STRING) USING parquet AS SELECT a FROM t1",
      "mismatched input 'AS'")
  }
  
  test("SPARK-17328 Fix NPE with EXPLAIN DESCRIBE TABLE") {
    assertEqual("describe table t",
      DescribeTableCommand(
        TableIdentifier("t"), Map.empty, isExtended = false, isFormatted = false))
    assertEqual("describe table extended t",
      DescribeTableCommand(
        TableIdentifier("t"), Map.empty, isExtended = true, isFormatted = false))
    assertEqual("describe table formatted t",
      DescribeTableCommand(
        TableIdentifier("t"), Map.empty, isExtended = false, isFormatted = true))

    intercept("explain describe tables x", "Unsupported SQL statement")
  }
}
