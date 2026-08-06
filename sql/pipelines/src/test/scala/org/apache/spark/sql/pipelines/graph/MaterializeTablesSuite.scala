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

package org.apache.spark.sql.pipelines.graph

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.connector.catalog.{
  CatalogV2Util,
  Identifier,
  InMemoryTableCatalog,
  Table => V2Table,
  TableCatalog,
  TableChange
}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, Expressions, FieldReference}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.graph.DatasetManager.TableMaterializationException
import org.apache.spark.sql.pipelines.utils.{BaseCoreExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils.exceptionString

class DefaultMaterializeTablesSuite extends MaterializeTablesSuite with SharedSparkSession

/**
 * Local integration tests for materialization of `Table`s in a `DataflowGraph` to make sure
 * tables are written with the appropriate schemas.
 */
abstract class MaterializeTablesSuite extends BaseCoreExecutionTest {
  import testImplicits._

  test("basic") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerMaterializedView(
          "a",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, nullable = false, "comment1")
              .add("x2", IntegerType, nullable = true, "comment2")
          ),
          comment = Option("p-comment"),
          query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val catalogTable = catalog.loadTable(identifier)

    assert(
      catalogTable.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x", IntegerType, nullable = false, "comment1")
          .add("x2", IntegerType, nullable = true, "comment2")
      )
    )
    assert(catalogTable.properties().get(TableCatalog.PROP_COMMENT) == "p-comment")

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerMaterializedView(
          "a",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, nullable = false, "comment3")
              .add("x2", IntegerType, nullable = true, "comment4")
          ),
          comment = Option("p-comment"),
          query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )
    val catalogTable2 = catalog.loadTable(identifier)
    assert(
      catalogTable2.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x", IntegerType, nullable = false, "comment3")
          .add("x2", IntegerType, nullable = true, "comment4")
      )
    )
    assert(catalogTable2.properties().get(TableCatalog.PROP_COMMENT) == "p-comment")

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerMaterializedView(
          "a",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, nullable = false)
              .add("x2", IntegerType, nullable = true)
          ),
          comment = Option("p-comment"),
          query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val catalogTable3 = catalog.loadTable(identifier)
    assert(
      catalogTable3.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x", IntegerType, nullable = false, comment = null)
          .add("x2", IntegerType, nullable = true, comment = null)
      )
    )
    assert(catalogTable3.properties().get(TableCatalog.PROP_COMMENT) == "p-comment")
  }

  test("multiple") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "t1",
          "t1",
          query = dfFlowFunc(Seq(1, 2, 3).toDF("x"))
        )
        registerFlow(
          "t2",
          "t2",
          query = dfFlowFunc(Seq("a", "b").toDF("y"))
        )
        registerTable("t1")
        registerTable("t2")
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val identifier1 = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t1")
    val identifier2 = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t2")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val catalogTable1 = catalog.loadTable(identifier1)
    val catalogTable2 = catalog.loadTable(identifier2)

    assert(
      catalogTable1.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("x", IntegerType))
    )
    assert(
      catalogTable2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("y", StringType))
    )
  }

  test("temporary views don't get materialized") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "t2",
          "t2",
          query = dfFlowFunc(Seq("a", "b").toDF("y"))
        )
        registerTable("t2")
        registerView(
          "t1",
          dfFlowFunc(Seq(1, 2, 3).toDF("x"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    assert(
      !catalog.tableExists(
        Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t1")
      )
    )
    assert(
      catalog.tableExists(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t2"))
    )
  }

  // TableManager performs different validations for batch tables vs streaming tables when
  // materializing tables. Flows writing to a batch tables can have incompatible schemas with the
  // existing table since the table is being overwritten completely. This test ensures that
  // it is possible to do that.
  test("batch flow reading from streaming table") {
    class P1 extends TestGraphRegistrationContext(spark) {
      registerTable(
        "a",
        query = Option(dfFlowFunc(spark.readStream.format("rate").load()))
      )
      // Defines a column called timestamp as `int`.
      registerMaterializedView(
        "b",
        query = sqlFlowFunc(spark, "SELECT value AS timestamp FROM a")
      )
    }
    materializeGraph(new P1().resolveToDataflowGraph(), storageRoot = storageRoot)

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val b =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    assert(
      b.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("timestamp", LongType))
    )

    class P2 extends TestGraphRegistrationContext(spark) {
      registerTable(
        "a",
        query = Option(dfFlowFunc(spark.readStream.format("rate").load()))
      )
      // Defines a column called timestamp as `timestamp`.
      registerMaterializedView(
        "b",
        query = sqlFlowFunc(spark, "SELECT timestamp FROM a")
      )
    }
    materializeGraph(new P2().resolveToDataflowGraph(), storageRoot = storageRoot)
    val b2 =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    assert(
      b2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("timestamp", TimestampType))
    )
  }

  test("schema matches existing table schema") {

    sql(s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t2(x INT)")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t2")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType().add("x", IntegerType)
      )
    )

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow("t2", "t2", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
        registerTable("t2")
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val table2 = catalog.loadTable(identifier)
    assert(
      table2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("x", IntegerType))
    )
  }

  test("invalid schema merge") {
    implicit val sqlCtx: SQLContext = spark.sqlContext

    val streamInts = MemoryStream[Int]
    streamInts.addData(1, 2)

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerView("a", query = dfFlowFunc(streamInts.toDF()))
        registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val streamStrings = MemoryStream[String]
    streamStrings.addData("a", "b")
    val graph2 = new TestGraphRegistrationContext(spark) {
      registerView("a", query = dfFlowFunc(streamStrings.toDF()))
      registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
    }.resolveToDataflowGraph()

    val ex = intercept[TableMaterializationException] {
      materializeGraph(graph2, storageRoot = storageRoot)
    }
    val cause = ex.cause
    val exStr = exceptionString(cause)
    assert(exStr.contains("Failed to merge incompatible data types"))
  }

  test("table materialized with specified schema, even if different from inferred") {

    sql(s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t4(x INT)")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t4")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType().add("x", IntegerType)
      )
    )

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerMaterializedView(
          "t4",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, nullable = true, "this is column x")
              .add("z", LongType, nullable = true, "this is column z")
          ),
          query = dfFlowFunc(Seq[Short](1, 2).toDF("x"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val table2 = catalog.loadTable(identifier)
    assert(
      table2.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x", IntegerType, nullable = true, "this is column x")
          .add("z", LongType, nullable = true, "this is column z")
      )
    )
  }

  test("specified schema incompatible with existing table") {
    implicit val sqlCtx: SQLContext = spark.sqlContext

    sql(s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t6(x BOOLEAN)")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t6")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType().add("x", BooleanType)
      )
    )

    val ex = intercept[TableMaterializationException] {
      materializeGraph(new TestGraphRegistrationContext(spark) {
        val source: MemoryStream[Int] = MemoryStream[Int]
        source.addData(1, 2)
        registerTable(
          "t6",
          specifiedSchema = Option(new StructType().add("x", IntegerType)),
          query = Option(dfFlowFunc(source.toDF().select($"value" as "x")))
        )

      }.resolveToDataflowGraph(), storageRoot = storageRoot)
    }
    val cause = ex.cause
    val exStr = exceptionString(cause)
    assert(exStr.contains("Failed to merge incompatible data types"))

    // Works fine for a complete table
    materializeGraph(new TestGraphRegistrationContext(spark) {
      registerMaterializedView(
        "t6",
        specifiedSchema = Option(new StructType().add("x", IntegerType)),
        query = dfFlowFunc(Seq(1, 2).toDF("x"))
      )
    }.resolveToDataflowGraph(),
    storageRoot = storageRoot)
    val table2 = catalog.loadTable(identifier)
    assert(
      table2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("x", IntegerType))
    )
  }

  test("partition columns with user schema") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "a",
          query = Option(dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x1", "x2"))),
          specifiedSchema = Option(
            new StructType()
              .add("x1", IntegerType)
              .add("x2", IntegerType)
          ),
          partitionCols = Option(Seq("x2"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType().add("x1", IntegerType).add("x2", IntegerType)
      )
    )
    assert(table.partitioning().toSeq == Seq(Expressions.identity("x2")))
  }

  test("specifying partition column with existing partitioned table") {

    sql(
      s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t7(x BOOLEAN, y INT) " +
      s"PARTITIONED BY (x)"
    )
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t7")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns().map(_.name()).toSet == new StructType()
        .add("x", BooleanType)
        .add("y", IntegerType)
        .fieldNames
        .toSet
    )
    assert(table.partitioning().toSeq == Seq(Expressions.identity("x")))

    // Specify the same partition column.
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "t7",
          "t7",
          query = dfFlowFunc(Seq((true, 1), (false, 3)).toDF("x", "y"))
        )
        registerTable(
          "t7",
          partitionCols = Option(Seq("x"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val table2 = catalog.loadTable(identifier)
    assert(
      table2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("y", IntegerType).add("x", BooleanType))
    )
    assert(table2.partitioning().toSeq == Seq(Expressions.identity("x")))

    // Don't specify any partition column; should throw.
    val ex = intercept[TableMaterializationException] {
      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerFlow(
            "t7",
            "t7",
            query = dfFlowFunc(Seq((true, 1), (false, 3)).toDF("x", "y"))
          )
          registerTable("t7")
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )
    }
    assert(ex.cause.asInstanceOf[SparkThrowable].getCondition == "CANNOT_UPDATE_PARTITION_COLUMNS")

    val table3 = catalog.loadTable(identifier)
    assert(
      table3.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("y", IntegerType).add("x", BooleanType))
    )
    assert(table3.partitioning().toSeq == Seq(Expressions.identity("x")))
  }

  test("specifying partition column different from existing partitioned table") {

    sql(
      s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t8(x BOOLEAN, y INT) " +
      s"PARTITIONED BY (x)"
    )

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t8")

    // Specify a different partition column. Should throw.
    val graph = new TestGraphRegistrationContext(spark) {
      registerFlow(
        "t8",
        "t8",
        query = dfFlowFunc(Seq((true, 1), (false, 3)).toDF("x", "y"))
      )
      registerTable("t8", partitionCols = Option(Seq("y")))
    }.resolveToDataflowGraph()

    val ex = intercept[TableMaterializationException] {
      materializeGraph(graph, storageRoot = storageRoot)
    }
    assert(ex.cause.asInstanceOf[SparkThrowable].getCondition == "CANNOT_UPDATE_PARTITION_COLUMNS")
    val table = catalog.loadTable(identifier)
    assert(table.partitioning().toSeq == Seq(Expressions.identity("x")))
  }

  test("Table properties are set when table gets materialized") {
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "a",
          query = Option(dfFlowFunc(spark.readStream.format("rate").load())),
          properties = Map(
            "pipelines.reset.allowed" -> "true",
            "some.prop" -> "foo"
          )
        )
        registerTable(
          "b",
          query = Option(sqlFlowFunc(spark, "SELECT * FROM STREAM a")),
          properties = Map("pipelines.reset.alloweD" -> "true", "some.prop" -> "foo")
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifierA = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a")
    val identifierB = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b")
    val tableA = catalog.loadTable(identifierA)
    val tableB = catalog.loadTable(identifierB)

    val expectedProps = Map(
      "pipelines.reset.allowed" -> "true",
      "some.prop" -> "foo"
    )

    assert(expectedProps.forall { case (k, v) => tableA.properties().asScala.get(k).contains(v) })
    assert(expectedProps.forall { case (k, v) => tableB.properties().asScala.get(k).contains(v) })
  }

  test("Invalid table properties error during table materialization") {

    // Invalid pipelines property
    val graph1 =
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "a",
          query = Option(dfFlowFunc(Seq(1).toDF())),
          properties = Map("pipelines.reset.allowed" -> "123")
        )
      }.resolveToDataflowGraph()
    val ex1 =
      intercept[TableMaterializationException] {
        materializeGraph(graph1, storageRoot = storageRoot)
      }

    assert(ex1.cause.isInstanceOf[IllegalArgumentException])
    assert(ex1.cause.getMessage.contains("pipelines.reset.allowed"))
  }

  test(
    "Materialization succeeds even if there are unknown pipeline properties on the existing table"
  ) {
    sql(
      s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t9(x INT) " +
      s"TBLPROPERTIES ('pipelines.someProperty' = 'foo')"
    )

    val graph1 = new TestGraphRegistrationContext(spark) {
      registerTable("a", query = Option(dfFlowFunc(spark.readStream.format("rate").load())))
    }.resolveToDataflowGraph().validate(spark.sessionState.conf.caseSensitiveAnalysis)

    materializeGraph(graph1, storageRoot = storageRoot)
  }

  for (isFullRefresh <- Seq(true, false)) {
    test(
      s"Complete tables should not evolve schema - isFullRefresh = $isFullRefresh"
    ) {
      val rawGraph =
        new TestGraphRegistrationContext(spark) {
          registerView("a", query = dfFlowFunc(Seq((1, 2), (2, 3)).toDF("x", "y")))
          registerMaterializedView("b", query = sqlFlowFunc(spark, "SELECT x FROM a"))
        }.resolveToDataflowGraph()

      val graph = materializeGraph(rawGraph, storageRoot = storageRoot)
      val (refreshSelection, fullRefreshSelection) = if (isFullRefresh) {
        (NoTables, AllTables)
      } else {
        (AllTables, NoTables)
      }

      materializeGraph(
        rawGraph,
        contextOpt = Option(
          TestPipelineUpdateContext(
            spark = spark,
            unresolvedGraph = graph,
            refreshTables = refreshSelection,
            fullRefreshTables = fullRefreshSelection,
            storageRoot = storageRoot
          )
        ),
        storageRoot = storageRoot
      )

      val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
      val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b")

      val table = catalog.loadTable(identifier)
      assert(
        table.columns() sameElements CatalogV2Util
          .structTypeToV2Columns(new StructType().add("x", IntegerType))
      )

      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerView("a", query = dfFlowFunc(Seq((1, 2), (2, 3)).toDF("x", "y")))
          registerMaterializedView("b", query = sqlFlowFunc(spark, "SELECT y FROM a"))
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )
      val table2 = catalog.loadTable(identifier)
      assert(
        table2.columns() sameElements CatalogV2Util
          .structTypeToV2Columns(new StructType().add("y", IntegerType))
      )
    }
  }

  for (isFullRefresh <- Seq(true, false)) {
    test(
      s"Streaming tables should evolve schema only if not full refresh = $isFullRefresh"
    ) {
      implicit val sqlCtx: SQLContext = spark.sqlContext

      val streamInts = MemoryStream[Int]
      streamInts.addData(1 until 5: _*)

      val graph =
        new TestGraphRegistrationContext(spark) {
          registerView("a", query = dfFlowFunc(streamInts.toDF()))
          registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
        }.resolveToDataflowGraph().validate(spark.sessionState.conf.caseSensitiveAnalysis)

      val (refreshSelection, fullRefreshSelection) = if (isFullRefresh) {
        (NoTables, AllTables)
      } else {
        (AllTables, NoTables)
      }
      val updateContextOpt = Option(
        TestPipelineUpdateContext(
          spark = spark,
          unresolvedGraph = graph,
          refreshTables = refreshSelection,
          fullRefreshTables = fullRefreshSelection,
          storageRoot = storageRoot
        )
      )
      materializeGraph(graph, contextOpt = updateContextOpt, storageRoot = storageRoot)

      val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
      val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b")
      val table = catalog.loadTable(identifier)
      assert(
        table.columns() sameElements CatalogV2Util
          .structTypeToV2Columns(new StructType().add("x", IntegerType))
      )

      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerView("a", query = dfFlowFunc(streamInts.toDF()))
          registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS y FROM STREAM a")))
        }.resolveToDataflowGraph().validate(spark.sessionState.conf.caseSensitiveAnalysis),
        contextOpt = updateContextOpt,
        storageRoot = storageRoot
      )

      val table2 = catalog.loadTable(identifier)

      if (isFullRefresh) {
        assert(
          table2.columns() sameElements CatalogV2Util.structTypeToV2Columns(
            new StructType().add("y", IntegerType)
          )
        )
      } else {
        assert(
          table2.columns() sameElements CatalogV2Util.structTypeToV2Columns(
            new StructType()
              .add("x", IntegerType)
              .add("y", IntegerType)
          )
        )
      }
    }
  }

  test(
    "materialize only selected tables"
  ) {

    val graph = new TestGraphRegistrationContext(spark) {
      registerTable("a", query = Option(dfFlowFunc(Seq((1, 2), (2, 3)).toDF("x", "y"))))
      registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT x FROM a")))
      registerTable("c", query = Option(sqlFlowFunc(spark, "SELECT y FROM a")))
    }.resolveToDataflowGraph()
    materializeGraph(
      graph,
      contextOpt = Option(
        TestPipelineUpdateContext(
          spark = spark,
          unresolvedGraph = graph,
          refreshTables = SomeTables(Set(fullyQualifiedIdentifier("a"))),
          fullRefreshTables = SomeTables(Set(fullyQualifiedIdentifier("c"))),
          storageRoot = storageRoot
        )
      ),
      storageRoot = storageRoot
    )

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]

    val tableA =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a"))
    assert(
      !catalog.tableExists(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    )
    val tableC =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "c"))

    assert(
      tableA.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x", IntegerType)
          .add("y", IntegerType)
      )
    )

    assert(
      tableC.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("y", IntegerType))
    )
  }

  test("tables with arrays and maps") {

    val rawGraph =
      new TestGraphRegistrationContext(spark) {
        registerTable("a", query = Option(sqlFlowFunc(spark, "select map(1, struct('a', 'b')) m")))
        registerTable(
          "b",
          query = Option(dfFlowFunc(Seq(Array(1, 3, 5), Array(2, 4, 6)).toDF("arr")))
        )
        registerTable(
          "c",
          query = Option(
            sqlFlowFunc(spark, "select * from a join b where map_entries(m)[0].key = arr[0]")
          )
        )
      }.resolveToDataflowGraph()
    materializeGraph(rawGraph, storageRoot = storageRoot)
    // Materialize twice because some logic compares the incoming schema with the previous one.
    materializeGraph(rawGraph, storageRoot = storageRoot)

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val tableA =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a"))
    val tableB =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    val tableC =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "c"))

    assert(
      tableA.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        StructType.fromDDL("m MAP<int, struct<col1: string, col2: string>>")
      )
    )
    assert(
      tableB.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        StructType.fromDDL("arr ARRAY<int>")
      )
    )
    assert(
      tableC.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        StructType.fromDDL("m MAP<int, struct<col1: string, col2: string>>, arr ARRAY<int>")
      )
    )
  }

  test("tables with nested arrays and maps") {
    val rawGraph =
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "a",
          query = Option(sqlFlowFunc(spark, "select map(0, map(0, struct('a', 'b'))) m"))
        )
        registerTable(
          "b",
          query = Option(
            sqlFlowFunc(spark, "select array(array('a', 'b', 'c'), array('d', 'e', 'f')) arr")
          )
        )
        registerTable(
          "c",
          query =
            Option(sqlFlowFunc(spark, "select * from a join b where m[0][0].col1 = arr[0][0]"))
        )

      }.resolveToDataflowGraph()
    materializeGraph(rawGraph, storageRoot = storageRoot)
    // Materialize twice because some logic compares the incoming schema with the previous one.
    materializeGraph(rawGraph, storageRoot = storageRoot)
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val tableA =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a"))
    val tableB =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    val tableC =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "c"))

    assert(
      tableA.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        StructType.fromDDL("m MAP<int, MAP<int, struct<col1: string, col2: string>>>")
      )
    )
    assert(
      tableB.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(StructType.fromDDL("arr ARRAY<ARRAY<string>>"))
    )
    assert(
      tableC.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        StructType.fromDDL(
          "m MAP<int, MAP<int, struct<col1: string, col2: string>>>, arr ARRAY<ARRAY<string>>"
        )
      )
    )
  }

  test("materializing no tables doesn't throw") {

    val graph1 =
      new DataflowGraph(flows = Seq.empty, tables = Seq.empty, views = Seq.empty, sinks = Seq.empty)
    val graph2 = new TestGraphRegistrationContext(spark) {
      registerFlow(
        "a",
        "a",
        query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
      )
      registerTable("a")
    }.resolveToDataflowGraph()

    materializeGraph(graph1, storageRoot = storageRoot)
    materializeGraph(
      graph2,
      contextOpt = Option(
        TestPipelineUpdateContext(
          spark = spark,
          unresolvedGraph = graph2,
          refreshTables = NoTables,
          fullRefreshTables = NoTables,
          storageRoot = storageRoot
        )
      ),
      storageRoot = storageRoot
    )
  }

  test("cluster columns with user schema") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "a",
          query = Option(dfFlowFunc(Seq((1, 1, "x"), (2, 3, "y")).toDF("x1", "x2", "x3"))),
          specifiedSchema = Option(
            new StructType()
              .add("x1", IntegerType)
              .add("x2", IntegerType)
              .add("x3", StringType)
          ),
          clusterCols = Option(Seq("x1", "x3"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x1", IntegerType)
          .add("x2", IntegerType)
          .add("x3", StringType)
      )
    )
    val expectedClusterTransform = ClusterByTransform(
      Seq(FieldReference("x1"), FieldReference("x3")).toSeq
    )
    assert(table.partitioning().contains(expectedClusterTransform))
  }

  test("specifying cluster column with existing clustered table") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "t10",
          query = Option(dfFlowFunc(Seq((1, true, "a"), (2, false, "b")).toDF("x", "y", "z"))),
          clusterCols = Option(Seq("x", "z"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t10")
    val table = catalog.loadTable(identifier)
    val expectedClusterTransform = ClusterByTransform(
      Seq(FieldReference("x"), FieldReference("z")).toSeq
    )
    assert(table.partitioning().contains(expectedClusterTransform))

    // Specify the same cluster columns - should work
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "t10",
          "t10",
          query = dfFlowFunc(Seq((3, true, "c"), (4, false, "d")).toDF("x", "y", "z"))
        )
        registerTable("t10", clusterCols = Option(Seq("x", "z")))
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val table2 = catalog.loadTable(identifier)
    assert(table2.partitioning().contains(expectedClusterTransform))

    // Don't specify cluster columns when table already has them - should throw
    val ex = intercept[TableMaterializationException] {
      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerFlow(
            "t10",
            "t10",
            query = dfFlowFunc(Seq((5, true, "e"), (6, false, "f")).toDF("x", "y", "z"))
          )
          registerTable("t10")
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )
    }
    assert(ex.cause.asInstanceOf[SparkThrowable].getCondition == "CANNOT_UPDATE_PARTITION_COLUMNS")
  }

  test("specifying cluster column different from existing clustered table") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "t11",
          query = Option(dfFlowFunc(Seq((1, true, "a"), (2, false, "b")).toDF("x", "y", "z"))),
          clusterCols = Option(Seq("x"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t11")

    // Specify different cluster columns - should throw
    val ex = intercept[TableMaterializationException] {
      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerFlow(
            "t11",
            "t11",
            query = dfFlowFunc(Seq((3, true, "c"), (4, false, "d")).toDF("x", "y", "z"))
          )
          registerTable("t11", clusterCols = Option(Seq("y")))
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )
    }
    assert(ex.cause.asInstanceOf[SparkThrowable].getCondition == "CANNOT_UPDATE_PARTITION_COLUMNS")

    val table = catalog.loadTable(identifier)
    val expectedClusterTransform = ClusterByTransform(Seq(FieldReference("x")).toSeq)
    assert(table.partitioning().contains(expectedClusterTransform))
  }

  test("cluster columns only (no partitioning)") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "t12",
          query = Option(dfFlowFunc(Seq((1, 1, "x"), (2, 3, "y")).toDF("x1", "x2", "x3"))),
          specifiedSchema = Option(
            new StructType()
              .add("x1", IntegerType)
              .add("x2", IntegerType)
              .add("x3", StringType)
          ),
          clusterCols = Option(Seq("x1", "x3"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t12")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x1", IntegerType)
          .add("x2", IntegerType)
          .add("x3", StringType)
      )
    )

    val transforms = table.partitioning()
    val expectedClusterTransform = ClusterByTransform(
      Seq(FieldReference("x1"), FieldReference("x3")).toSeq
    )
    assert(transforms.contains(expectedClusterTransform))
  }

  test("materialized view with cluster columns") {

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerMaterializedView(
          "mv1",
          query = dfFlowFunc(Seq((1, 1, "x"), (2, 3, "y")).toDF("x1", "x2", "x3")),
          clusterCols = Option(Seq("x1", "x2"))
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "mv1")
    val table = catalog.loadTable(identifier)
    assert(
      table.columns() sameElements CatalogV2Util.structTypeToV2Columns(
        new StructType()
          .add("x1", IntegerType)
          .add("x2", IntegerType)
          .add("x3", StringType)
      )
    )
    val expectedClusterTransform = ClusterByTransform(
      Seq(FieldReference("x1"), FieldReference("x2")).toSeq
    )
    assert(table.partitioning().contains(expectedClusterTransform))
  }

  test("partition and cluster columns together should fail") {

    val ex = intercept[TableMaterializationException] {
      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerTable(
            "invalid_table",
            query = Option(dfFlowFunc(Seq((1, 1, "x"), (2, 3, "y")).toDF("x1", "x2", "x3"))),
            partitionCols = Option(Seq("x2")),
            clusterCols = Option(Seq("x1", "x3"))
          )
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )
    }
    assert(ex.cause.isInstanceOf[AnalysisException])
    val analysisEx = ex.cause.asInstanceOf[AnalysisException]
    assert(analysisEx.errorClass.get == "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED")
  }

  test("cluster column that doesn't exist in table schema should fail") {

    val ex = intercept[TableMaterializationException] {
      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerTable(
            "invalid_cluster_table",
            query = Option(dfFlowFunc(Seq((1, 1, "x"), (2, 3, "y")).toDF("x1", "x2", "x3"))),
            clusterCols = Option(Seq("nonexistent_column"))
          )
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )
    }
    assert(ex.cause.isInstanceOf[AnalysisException])
  }

  // =============== Table evolution in catalog tests ===============

  private val recordingCatalogName = "recording_cat"
  private val recordingNamespace = "rec_ns"

  /**
   * Registers [[RecordingInMemoryTableCatalog]] under `recordingCatalogName`, creates
   * `recordingNamespace`, runs `body`, then tears the registration back down.
   */
  private def withRecordingCatalog(body: => Unit): Unit = {
    spark.conf.set(
      s"spark.sql.catalog.$recordingCatalogName",
      classOf[RecordingInMemoryTableCatalog].getName
    )
    try {
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $recordingCatalogName.$recordingNamespace")
      body
    } finally {
      spark.sessionState.catalogManager.reset()
      spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$recordingCatalogName")
    }
  }

  /**
   * Materializes a single streaming table under the recording catalog/namespace with the given
   * schema and properties.
   */
  private def materializeStreamingTable(
      name: String,
      schema: StructType,
      properties: Map[String, String]): Unit = {
    // All nulls dummy row, compatible with any schema type
    val row = Row.fromSeq(Seq.fill(schema.length)(null))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          name,
          query = Option(dfFlowFunc(df)),
          specifiedSchema = Option(schema),
          properties = properties,
          catalog = Option(recordingCatalogName),
          database = Option(recordingNamespace)
        )
      }.resolveToDataflowGraph(),
      storageRoot = storageRoot
    )
  }

  private def recordingCatalog: RecordingInMemoryTableCatalog =
    spark.sessionState.catalogManager
      .catalog(recordingCatalogName)
      .asInstanceOf[RecordingInMemoryTableCatalog]

  private def loadTableFromRecordingCatalog(name: String): V2Table = {
    val catalog = spark.sessionState.catalogManager
      .catalog(recordingCatalogName)
      .asInstanceOf[TableCatalog]
    catalog.loadTable(Identifier.of(Array(recordingNamespace), name))
  }

  test("re-materializing an unchanged table does not issue an alterTable") {
    withRecordingCatalog {
      val schema = new StructType().add("id", IntegerType).add("value", StringType)
      val props = Map("p.a" -> "1", "p.b" -> "2")
      // Creating the table issues no alter, and re-materializing the unchanged table is a no-op,
      // so no alter is ever recorded.
      materializeStreamingTable("t", schema, props)
      assert(recordingCatalog.recordedAlters.isEmpty)
      materializeStreamingTable("t", schema, props)
      assert(recordingCatalog.recordedAlters.isEmpty)
    }
  }

  test("re-materializing with changed/new properties issues an alterTable that sets them") {
    withRecordingCatalog {
      val schema = new StructType().add("id", IntegerType).add("value", StringType)
      // Creating the table issues no alter; re-materializing with changed/added properties issues
      // exactly one alter that sets them.
      materializeStreamingTable("t", schema, Map("p.a" -> "1"))
      assert(recordingCatalog.recordedAlters.isEmpty)
      materializeStreamingTable("t", schema, Map("p.a" -> "2", "p.new" -> "n"))
      assert(recordingCatalog.recordedAlters.size == 1)

      val changes = recordingCatalog.recordedAlters.flatten
      assert(changes.forall(_.isInstanceOf[TableChange.SetProperty]))
      val set = changes.collect {
        case s: TableChange.SetProperty => s.property() -> s.value()
      }.toMap
      assert(set == Map("p.a" -> "2", "p.new" -> "n"))

      val table = loadTableFromRecordingCatalog("t")
      assert(table.properties().get("p.a") == "2")
      assert(table.properties().get("p.new") == "n")
    }
  }

  test("re-materializing with an added column issues an alterTable") {
    withRecordingCatalog {
      // Creating the table issues no alter; re-materializing with an added column issues exactly
      // one alter that adds it.
      materializeStreamingTable("t", new StructType().add("id", IntegerType), Map("p.a" -> "1"))
      assert(recordingCatalog.recordedAlters.isEmpty)
      materializeStreamingTable(
        "t",
        new StructType().add("id", IntegerType).add("value", StringType),
        Map("p.a" -> "1")
      )
      assert(recordingCatalog.recordedAlters.size == 1)

      val changes = recordingCatalog.recordedAlters.flatten
      assert(changes.exists(_.isInstanceOf[TableChange.AddColumn]))

      assert(
        loadTableFromRecordingCatalog("t").columns() sameElements
          CatalogV2Util.structTypeToV2Columns(
            new StructType().add("id", IntegerType).add("value", StringType)
          )
      )
    }
  }

  test("SPARK-58517: re-materializing with a case-only column difference is a no-op under " +
    "case-insensitive resolution") {
    withRecordingCatalog {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        // Create the table with `value`, then re-materialize with the same column cased as `Value`.
        // Under case-insensitive resolution these are the same column, so schema evolution must
        // fold `Value` onto the existing `value`: no alterTable, and the persisted column keeps its
        // original name/case. Before SPARK-58517 the case-sensitive merge instead added a second
        // `Value` column, corrupting the table.
        materializeStreamingTable(
          "t", new StructType().add("id", IntegerType).add("value", StringType), Map.empty)
        assert(recordingCatalog.recordedAlters.isEmpty)

        materializeStreamingTable(
          "t", new StructType().add("id", IntegerType).add("Value", StringType), Map.empty)
        assert(recordingCatalog.recordedAlters.isEmpty,
          s"expected no alter, got: ${recordingCatalog.recordedAlters}")

        assert(
          loadTableFromRecordingCatalog("t").columns() sameElements
            CatalogV2Util.structTypeToV2Columns(
              new StructType().add("id", IntegerType).add("value", StringType)
            ),
          "the persisted schema should keep the original `value` column, not gain a `Value` column"
        )
      }
    }
  }

  test("SPARK-58517: multi-flow schema inference folds case-only column differences under " +
    "case-insensitive resolution") {
    withRecordingCatalog {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        // Two append flows write to the same streaming table, one emitting `value` and the other
        // `Value`. The target schema is INFERRED by merging the flows' schemas, which happens
        // before the evolveTable path runs -- so inference must honor case-insensitivity too,
        // otherwise the table is created with both columns and the engine's own resolver cannot
        // disambiguate them.
        val df1 = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1, "a"))),
          new StructType().add("id", IntegerType).add("value", StringType))
        val df2 = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(2, "b"))),
          new StructType().add("id", IntegerType).add("Value", StringType))

        val ctx = new TestGraphRegistrationContext(spark) {
          registerTable(
            "t",
            catalog = Option(recordingCatalogName),
            database = Option(recordingNamespace))
          registerFlow(
            "t", "f1", dfFlowFunc(df1),
            catalog = Option(recordingCatalogName), database = Option(recordingNamespace))
          registerFlow(
            "t", "f2", dfFlowFunc(df2),
            catalog = Option(recordingCatalogName), database = Option(recordingNamespace))
        }

        val graph = ctx.resolveToDataflowGraph()
        val inferredSchemas = graph.inferSchemas(spark.sessionState.conf.caseSensitiveAnalysis)
        val (targetIdentifier, inferred) = inferredSchemas.head
        val lowestIdentifierFlowValueField =
          graph.resolvedFlowsTo(targetIdentifier)
            .sortBy(_.identifier.unquotedString)
            .head
            .schema
            .fieldNames(1)
        // The two spellings must fold into a single column, and the lowest flow identifier
        // supplies the surviving spelling.
        assert(inferred.fieldNames.toSeq === Seq("id", lowestIdentifierFlowValueField))
      }
    }
  }

  test("SPARK-58517: a case-only fold picks the same spelling for the materialized table and for " +
    "downstream resolution, in either flow declaration order") {
    // The table's schema is derived twice from the same flows, by two different callers: the graph
    // materializes the table from `inferSchemas`, while downstream flows resolve against the
    // `VirtualTableInput` schema, whose `availableFlows` is in declaration order, not sorted. Both
    // go through `inferSchemaFromFlows`, which merges in sorted flow identifier order, so the
    // surviving spelling is the same on both paths and does not depend on declaration order. Were
    // the two to disagree, the downstream view would persist a column spelled differently from the
    // source column it selects, and -- because `diffSchemas` keys on exact names -- reordering the
    // flow definitions would turn the next refresh into a drop-then-add of that column.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      // `f_eu` sorts before `f_us`, so `value` is the surviving spelling in both orders.
      def graphWithFlowsDeclared(usFirst: Boolean): DataflowGraph = {
        val usDf = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1, "a"))),
          new StructType().add("id", IntegerType).add("Value", StringType))
        val euDf = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(2, "b"))),
          new StructType().add("id", IntegerType).add("value", StringType))

        new TestGraphRegistrationContext(spark) {
          registerTable("events")
          val registerUs = () => registerFlow("events", "f_us", dfFlowFunc(usDf))
          val registerEu = () => registerFlow("events", "f_eu", dfFlowFunc(euDf))
          if (usFirst) {
            registerUs()
            registerEu()
          } else {
            registerEu()
            registerUs()
          }
          // Reads the table, so it resolves against the VirtualTableInput schema.
          registerMaterializedView(
            "events_summary",
            query = sqlFlowFunc(spark, "SELECT id, value FROM events"))
        }.resolveToDataflowGraph()
      }

      Seq(true, false).foreach { usFirst =>
        val graph = graphWithFlowsDeclared(usFirst)
        val sessionCaseSensitive = spark.sessionState.conf.caseSensitiveAnalysis
        val eventsIdentifier = fullyQualifiedIdentifier("events")

        val materializedSchema = graph.inferSchemas(sessionCaseSensitive)(eventsIdentifier)
        assert(
          materializedSchema.fieldNames.toSeq === Seq("id", "value"),
          s"materialized schema for usFirst=$usFirst")

        // The downstream view's own schema reflects what it resolved against upstream: Spark takes
        // the resolved attribute's name, so a `Value` upstream would surface here as `Value`.
        val summarySchema = graph
          .inferSchemas(sessionCaseSensitive)(fullyQualifiedIdentifier("events_summary"))
        assert(
          summarySchema.fieldNames.toSeq === Seq("id", "value"),
          s"downstream schema for usFirst=$usFirst")
      }
    }
  }

  test("multi-flow schema inference keeps case-only column differences distinct under " +
    "case-sensitive resolution") {
    withRecordingCatalog {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        // The case-sensitive control: `value` and `Value` are distinct columns, so inference
        // contributes both.
        val df1 = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1, "a"))),
          new StructType().add("id", IntegerType).add("value", StringType))
        val df2 = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(2, "b"))),
          new StructType().add("id", IntegerType).add("Value", StringType))

        val ctx = new TestGraphRegistrationContext(spark) {
          registerTable(
            "t",
            catalog = Option(recordingCatalogName),
            database = Option(recordingNamespace))
          registerFlow(
            "t", "f1", dfFlowFunc(df1),
            catalog = Option(recordingCatalogName), database = Option(recordingNamespace))
          registerFlow(
            "t", "f2", dfFlowFunc(df2),
            catalog = Option(recordingCatalogName), database = Option(recordingNamespace))
        }

        val graph = ctx.resolveToDataflowGraph()
        val inferred = graph.inferSchemas(
          spark.sessionState.conf.caseSensitiveAnalysis).values.head
        // Both spellings survive as distinct columns in sorted flow identifier order.
        assert(inferred.fieldNames.toSeq === Seq("id", "value", "Value"))
      }
    }
  }

  test("SPARK-58517: a materialized view's case-only column rename is applied under " +
    "case-insensitive resolution") {
    // The non-merging path: for a materialized view `targetSchema` is the run's declared schema
    // as-is (no merge with the persisted schema), so a case-only rename must remain visible to
    // `diffSchemas` as a drop-then-add. Case-insensitive matching here would emit no change at all
    // and freeze the persisted spelling forever -- the table would permanently disagree with its
    // definition, with no error pointing at the discrepancy, and a colleague materializing the same
    // definition against a fresh table would get the declared casing instead.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerView("src", query = dfFlowFunc(Seq((1, 2L)).toDF("id", "total")))
          registerMaterializedView("mv", query = sqlFlowFunc(spark, "SELECT id, total FROM src"))
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )

      val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
      val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "mv")
      assert(
        catalog.loadTable(identifier).columns() sameElements CatalogV2Util.structTypeToV2Columns(
          new StructType().add("id", IntegerType).add("total", LongType))
      )

      // Re-materialize with the column cased as `Total`. The table must follow the definition.
      materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerView("src", query = dfFlowFunc(Seq((1, 2L)).toDF("id", "total")))
          registerMaterializedView(
            "mv", query = sqlFlowFunc(spark, "SELECT id, total AS Total FROM src"))
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )
      assert(
        catalog.loadTable(identifier).columns() sameElements CatalogV2Util.structTypeToV2Columns(
          new StructType().add("id", IntegerType).add("Total", LongType)),
        "the materialized view should adopt the declared `Total` casing, not keep `total`"
      )
    }
  }

  test("SPARK-58517: a full-refreshed streaming table's case-only column rename is applied " +
    "under case-insensitive resolution") {
    // The streaming-table analog of the materialized-view case above: a full refresh also takes
    // `targetSchema` as the declared schema without merging, so the same case-only rename must be
    // applied rather than silently ignored.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val graph = materializeGraph(
        new TestGraphRegistrationContext(spark) {
          registerView("src", query = dfFlowFunc(Seq((1, 2L)).toDF("id", "total")))
          registerTable("st", query = Option(sqlFlowFunc(spark, "SELECT id, total FROM src")))
        }.resolveToDataflowGraph(),
        storageRoot = storageRoot
      )

      val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
      val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "st")
      assert(
        catalog.loadTable(identifier).columns() sameElements CatalogV2Util.structTypeToV2Columns(
          new StructType().add("id", IntegerType).add("total", LongType))
      )

      val renamedGraph =
        new TestGraphRegistrationContext(spark) {
          registerView("src", query = dfFlowFunc(Seq((1, 2L)).toDF("id", "total")))
          registerTable(
            "st", query = Option(sqlFlowFunc(spark, "SELECT id, total AS Total FROM src")))
        }.resolveToDataflowGraph()

      materializeGraph(
        renamedGraph,
        contextOpt = Option(
          TestPipelineUpdateContext(
            spark = spark,
            unresolvedGraph = graph,
            refreshTables = NoTables,
            fullRefreshTables = AllTables,
            storageRoot = storageRoot
          )
        ),
        storageRoot = storageRoot
      )
      assert(
        catalog.loadTable(identifier).columns() sameElements CatalogV2Util.structTypeToV2Columns(
          new StructType().add("id", IntegerType).add("Total", LongType)),
        "a full-refreshed streaming table should adopt the declared `Total` casing"
      )
    }
  }

  test("re-materializing with a case-only column difference adds a column under case-sensitive " +
    "resolution") {
    withRecordingCatalog {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        // The case-sensitive counterpart: `value` and `Value` are distinct, so `Value` is added.
        materializeStreamingTable(
          "t", new StructType().add("id", IntegerType).add("value", StringType), Map.empty)
        assert(recordingCatalog.recordedAlters.isEmpty)

        materializeStreamingTable(
          "t", new StructType().add("id", IntegerType).add("Value", StringType), Map.empty)
        assert(recordingCatalog.recordedAlters.size == 1)
        val changes = recordingCatalog.recordedAlters.flatten
        assert(changes.collect { case ac: TableChange.AddColumn => ac.fieldNames()(0) } ==
          Seq("Value"))
      }
    }
  }

  test("SPARK-58517: schema evolution uses the pipeline's case sensitivity, not the session's") {
    // A pipeline-level `SET spark.sql.caseSensitive` never reaches the session, so evolution must
    // read it from the flows. Here the session default is case-INsensitive while the pipeline asks
    // for case-SENSITIVE, so `Value` must become its own column alongside the persisted `value` --
    // matching how the flow itself resolves the name.
    withRecordingCatalog {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        materializeGraph(
          new TestGraphRegistrationContext(
            spark, Map(SQLConf.CASE_SENSITIVE.key -> "true")) {
            registerView("src", query = dfFlowFunc(Seq((1, "a")).toDF("id", "value")))
            registerTable(
              "t",
              query = Option(sqlFlowFunc(spark, "SELECT id, value FROM src")),
              catalog = Option(recordingCatalogName),
              database = Option(recordingNamespace))
          }.resolveToDataflowGraph(),
          storageRoot = storageRoot
        )
        assert(
          loadTableFromRecordingCatalog("t").columns() sameElements
            CatalogV2Util.structTypeToV2Columns(
              new StructType().add("id", IntegerType).add("value", StringType)))

        materializeGraph(
          new TestGraphRegistrationContext(
            spark, Map(SQLConf.CASE_SENSITIVE.key -> "true")) {
            registerView("src", query = dfFlowFunc(Seq((1, "a")).toDF("id", "value")))
            registerTable(
              "t",
              query = Option(sqlFlowFunc(spark, "SELECT id, value AS Value FROM src")),
              catalog = Option(recordingCatalogName),
              database = Option(recordingNamespace))
          }.resolveToDataflowGraph(),
          storageRoot = storageRoot
        )
        assert(
          loadTableFromRecordingCatalog("t").columns() sameElements
            CatalogV2Util.structTypeToV2Columns(
              new StructType()
                .add("id", IntegerType)
                .add("value", StringType)
                .add("Value", StringType)),
          "the pipeline asked for case-sensitive resolution, so `Value` must be its own column")
      }
    }
  }

  test("SPARK-58517: flows writing to one table that disagree on case sensitivity are rejected") {
    // The effective value decides whether names differing only in case identify the same column, so
    // if the flows disagree the resulting schema would depend on the order they are evaluated in.
    // Fail with a clear error instead of picking one arbitrarily.
    val ctx = new TestGraphRegistrationContext(spark) {
      registerView("src", query = dfFlowFunc(Seq((1, "a")).toDF("id", "value")))
      registerTable("t")
      registerFlow(
        "t", "f1", sqlFlowFunc(spark, "SELECT id, value FROM src"),
        sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "true"))
      registerFlow(
        "t", "f2", sqlFlowFunc(spark, "SELECT id, value AS Value FROM src"),
        sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "false"))
    }

    val ex = intercept[AnalysisException] {
      ctx.resolveToDataflowGraph().inferSchemas(
        spark.sessionState.conf.caseSensitiveAnalysis)
    }
    checkError(
      exception = ex,
      condition = "CONFLICTING_PIPELINE_FLOW_CASE_SENSITIVITY",
      parameters = Map(
        "tableName" -> "spark_catalog.test_db.t",
        "configKey" -> SQLConf.CASE_SENSITIVE.key,
        "flowConfigurations" ->
          ("false (spark_catalog.test_db.f2); true (spark_catalog.test_db.f1)")
      )
    )
  }

  test("SPARK-58517: a flow inheriting the session value conflicts with one that overrides it") {
    // f2 leaves the conf unset, so it inherits the session's case-INsensitive default, which
    // conflicts with f1's explicit case-sensitive request just as much as an opposite explicit
    // value would.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val ctx = new TestGraphRegistrationContext(spark) {
        registerView("src", query = dfFlowFunc(Seq((1, "a")).toDF("id", "value")))
        registerTable("t")
        registerFlow(
          "t", "f1", sqlFlowFunc(spark, "SELECT id, value FROM src"),
          sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "true"))
        registerFlow("t", "f2", sqlFlowFunc(spark, "SELECT id, value FROM src"))
      }

      val ex = intercept[AnalysisException] {
        ctx.resolveToDataflowGraph().inferSchemas(
          spark.sessionState.conf.caseSensitiveAnalysis)
      }
      assert(ex.getCondition === "CONFLICTING_PIPELINE_FLOW_CASE_SENSITIVITY")
      assert(ex.getMessage.contains("session default"))
    }
  }

  test("SPARK-58517: flows that agree on case sensitivity are accepted") {
    // The negative control: identical explicit values are not a conflict, and neither is a value
    // that merely differs in spelling from the session's ("TRUE" vs "true").
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val ctx = new TestGraphRegistrationContext(spark) {
        registerView("src", query = dfFlowFunc(Seq((1, "a")).toDF("id", "value")))
        registerTable("t")
        registerFlow(
          "t", "f1", sqlFlowFunc(spark, "SELECT id, value FROM src"),
          sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "true"))
        registerFlow(
          "t", "f2", sqlFlowFunc(spark, "SELECT id, value AS Value FROM src"),
          sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "TRUE"))
      }
      val inferred = ctx.resolveToDataflowGraph()
        .inferSchemas(spark.sessionState.conf.caseSensitiveAnalysis)(
          fullyQualifiedIdentifier("t"))
      // Case-sensitive, so both spellings survive.
      assert(inferred.fieldNames.toSeq === Seq("id", "value", "Value"))
    }
  }

  test("re-materializing with a dropped property neither removes it nor issues an alterTable") {
    withRecordingCatalog {
      val schema = new StructType().add("id", IntegerType)
      // This test locks in the current buggy behavior where dropped properties do not materialize
      // against the catalog table entity. See SPARK-57670.
      materializeStreamingTable("t", schema, Map("p.keep" -> "v", "p.stale" -> "old"))
      assert(recordingCatalog.recordedAlters.isEmpty)
      materializeStreamingTable("t", schema, Map("p.keep" -> "v"))
      assert(recordingCatalog.recordedAlters.isEmpty)

      assert(loadTableFromRecordingCatalog("t").properties().get("p.stale") == "old")
    }
  }
}

/**
 * An [[InMemoryTableCatalog]] that records every `alterTable` invocation while still applying it,
 * so tests can assert whether materialization issued an alter or skipped it as a no-op.
 */
class RecordingInMemoryTableCatalog extends InMemoryTableCatalog {
  val recordedAlters: mutable.ArrayBuffer[Seq[TableChange]] = mutable.ArrayBuffer.empty

  override def alterTable(ident: Identifier, changes: TableChange*): V2Table = {
    recordedAlters += changes.toSeq
    super.alterTable(ident, changes: _*)
  }
}
