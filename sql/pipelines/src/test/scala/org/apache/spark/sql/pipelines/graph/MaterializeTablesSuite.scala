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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.execution.streaming.MemoryStream
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
  test("basic") {
    val session = spark
    import session.implicits._

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
      }.resolveToDataflowGraph()
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
      }.resolveToDataflowGraph()
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
      }.resolveToDataflowGraph()
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
    val session = spark
    import session.implicits._

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
      }.resolveToDataflowGraph()
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
    val session = spark
    import session.implicits._

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
      }.resolveToDataflowGraph()
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
    materializeGraph(new P1().resolveToDataflowGraph())

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
    materializeGraph(new P2().resolveToDataflowGraph())
    val b2 =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    assert(
      b2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("timestamp", TimestampType))
    )
  }

  test("schema matches existing table schema") {
    val session = spark
    import session.implicits._

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
      }.resolveToDataflowGraph()
    )

    val table2 = catalog.loadTable(identifier)
    assert(
      table2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("x", IntegerType))
    )
  }

  test("invalid schema merge") {
    val session = spark
    import session.implicits._

    val streamInts = MemoryStream[Int]
    streamInts.addData(1, 2)

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerView("a", query = dfFlowFunc(streamInts.toDF()))
        registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
      }.resolveToDataflowGraph()
    )

    val streamStrings = MemoryStream[String]
    streamStrings.addData("a", "b")
    val graph2 = new TestGraphRegistrationContext(spark) {
      registerView("a", query = dfFlowFunc(streamStrings.toDF()))
      registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
    }.resolveToDataflowGraph()

    val ex = intercept[TableMaterializationException] {
      materializeGraph(graph2)
    }
    val cause = ex.cause
    val exStr = exceptionString(cause)
    assert(exStr.contains("Failed to merge incompatible data types"))
  }

  test("table materialized with specified schema, even if different from inferred") {
    val session = spark
    import session.implicits._

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
      }.resolveToDataflowGraph()
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
    val session = spark
    import session.implicits._

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

      }.resolveToDataflowGraph())
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
    }.resolveToDataflowGraph())
    val table2 = catalog.loadTable(identifier)
    assert(
      table2.columns() sameElements CatalogV2Util
        .structTypeToV2Columns(new StructType().add("x", IntegerType))
    )
  }

  test("partition columns with user schema") {
    val session = spark
    import session.implicits._

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
      }.resolveToDataflowGraph()
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
    val session = spark
    import session.implicits._

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
      }.resolveToDataflowGraph()
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
        }.resolveToDataflowGraph()
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
    val session = spark
    import session.implicits._

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
      materializeGraph(graph)
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
      }.resolveToDataflowGraph()
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
    val session = spark
    import session.implicits._

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
        materializeGraph(graph1)
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
    }.resolveToDataflowGraph().validate()

    materializeGraph(graph1)
  }

  for (isFullRefresh <- Seq(true, false)) {
    test(
      s"Complete tables should not evolve schema - isFullRefresh = $isFullRefresh"
    ) {
      val session = spark
      import session.implicits._

      val rawGraph =
        new TestGraphRegistrationContext(spark) {
          registerView("a", query = dfFlowFunc(Seq((1, 2), (2, 3)).toDF("x", "y")))
          registerMaterializedView("b", query = sqlFlowFunc(spark, "SELECT x FROM a"))
        }.resolveToDataflowGraph()

      val graph = materializeGraph(rawGraph)
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
            fullRefreshTables = fullRefreshSelection
          )
        )
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
        }.resolveToDataflowGraph()
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
      val session = spark
      import session.implicits._

      val streamInts = MemoryStream[Int]
      streamInts.addData(1 until 5: _*)

      val graph =
        new TestGraphRegistrationContext(spark) {
          registerView("a", query = dfFlowFunc(streamInts.toDF()))
          registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
        }.resolveToDataflowGraph().validate()

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
          fullRefreshTables = fullRefreshSelection
        )
      )
      materializeGraph(graph, contextOpt = updateContextOpt)

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
        }.resolveToDataflowGraph().validate(),
        contextOpt = updateContextOpt
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
    val session = spark
    import session.implicits._

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
          fullRefreshTables = SomeTables(Set(fullyQualifiedIdentifier("c")))
        )
      )
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
    val session = spark
    import session.implicits._

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
    materializeGraph(rawGraph)
    // Materialize twice because some logic compares the incoming schema with the previous one.
    materializeGraph(rawGraph)

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
    materializeGraph(rawGraph)
    // Materialize twice because some logic compares the incoming schema with the previous one.
    materializeGraph(rawGraph)
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
    val session = spark
    import session.implicits._

    val graph1 =
      new DataflowGraph(flows = Seq.empty, tables = Seq.empty, views = Seq.empty)
    val graph2 = new TestGraphRegistrationContext(spark) {
      registerFlow(
        "a",
        "a",
        query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
      )
      registerTable("a")
    }.resolveToDataflowGraph()

    materializeGraph(graph1)
    materializeGraph(
      graph2,
      contextOpt = Option(
        TestPipelineUpdateContext(
          spark = spark,
          unresolvedGraph = graph2,
          refreshTables = NoTables,
          fullRefreshTables = NoTables
        )
      )
    )
  }
}
