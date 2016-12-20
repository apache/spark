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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

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

  test("create table - schema") {
    assertEqual("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING)",
      createTable(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType, nullable = true, "test")
          .add("b", StringType)
      )
    )
    assertEqual("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) " +
      "PARTITIONED BY (c INT, d STRING COMMENT 'test2')",
      createTable(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType, nullable = true, "test")
          .add("b", StringType)
          .add("c", IntegerType)
          .add("d", StringType, nullable = true, "test2"),
        partitionColumnNames = Seq("c", "d")
      )
    )
    assertEqual("CREATE TABLE my_tab(id BIGINT, nested STRUCT<col1: STRING,col2: INT>)",
      createTable(
        table = "my_tab",
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
    assertEqual("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) " +
      "PARTITIONED BY (nested STRUCT<col1: STRING,col2: INT>)",
      createTable(
        table = "my_tab",
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
    intercept("CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING)",
      "no viable alternative at input")
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

  test("analyze table statistics") {
    assertEqual("analyze table t compute statistics",
      AnalyzeTableCommand(TableIdentifier("t"), noscan = false))
    assertEqual("analyze table t compute statistics noscan",
      AnalyzeTableCommand(TableIdentifier("t"), noscan = true))
    assertEqual("analyze table t partition (a) compute statistics nOscAn",
      AnalyzeTableCommand(TableIdentifier("t"), noscan = true))

    // Partitions specified - we currently parse them but don't do anything with it
    assertEqual("ANALYZE TABLE t PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS",
      AnalyzeTableCommand(TableIdentifier("t"), noscan = false))
    assertEqual("ANALYZE TABLE t PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS noscan",
      AnalyzeTableCommand(TableIdentifier("t"), noscan = true))
    assertEqual("ANALYZE TABLE t PARTITION(ds, hr) COMPUTE STATISTICS",
      AnalyzeTableCommand(TableIdentifier("t"), noscan = false))
    assertEqual("ANALYZE TABLE t PARTITION(ds, hr) COMPUTE STATISTICS noscan",
      AnalyzeTableCommand(TableIdentifier("t"), noscan = true))

    intercept("analyze table t compute statistics xxxx",
      "Expected `NOSCAN` instead of `xxxx`")
    intercept("analyze table t partition (a) compute statistics xxxx",
      "Expected `NOSCAN` instead of `xxxx`")
  }

  test("analyze table column statistics") {
    intercept("ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS", "")

    assertEqual("ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS key, value",
      AnalyzeColumnCommand(TableIdentifier("t"), Seq("key", "value")))
  }
}
