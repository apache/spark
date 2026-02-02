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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.common.FlowStatus
import org.apache.spark.sql.pipelines.logging.EventLevel
import org.apache.spark.sql.pipelines.utils.ExecutionTest
import org.apache.spark.sql.test.SharedSparkSession

class ViewSuite extends ExecutionTest with SharedSparkSession {
  private val externalTable1Ident = fullyQualifiedIdentifier("external_t1")

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create mock external tables that tests can reference, ex. to stream from.
    spark.sql(s"CREATE TABLE $externalTable1Ident AS SELECT * FROM RANGE(3)")
  }

  override def afterEach(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $externalTable1Ident")
    super.afterEach()
  }


  test("create persisted views") {
    val session = spark
    import session.implicits._

    val viewName = "mypersistedview"
    val viewIdentifier = fullyQualifiedIdentifier(viewName)

    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"CREATE VIEW $viewName AS SELECT * FROM range(1, 4);"
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val executedGraph = updateContext.pipelineExecution.graphExecution.get.graphForExecution
    verifyPersistedViewMetadata(
      graph = executedGraph,
      viewMetadata = Map(
        viewIdentifier -> "SELECT * FROM range(1, 4)"
      )
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $viewIdentifier"),
      Seq(1L, 2L, 3L).toDF()
    )
  }

  test("persisted view reads from external table") {
    val session = spark
    import session.implicits._

    val viewName = "mypersistedview"
    val viewIdentifier = fullyQualifiedIdentifier(viewName)

    val tableIdentifier = fullyQualifiedIdentifier("mytable")

    spark.sql(s"CREATE TABLE $tableIdentifier AS SELECT * FROM range(1, 4)")

    val source = tableIdentifier.toString
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"CREATE VIEW $viewName AS SELECT * FROM $source;"
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graph = updateContext.pipelineExecution.graphExecution.get.graphForExecution

    verifyPersistedViewMetadata(
      graph = graph,
      viewMetadata = Map(
        viewIdentifier -> s"SELECT * FROM $source"
      )
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $viewIdentifier"),
      Seq(1L, 2L, 3L).toDF()
    )
  }

  test("persisted view reads from a temporary view") {
    val viewName = "pv"
    val pv = fullyQualifiedIdentifier(viewName)

    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""CREATE TEMPORARY VIEW temp_view AS SELECT * FROM range(1, 4);
                   |CREATE VIEW $viewName AS SELECT * FROM temp_view;""".stripMargin
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)

    val ex = intercept[AnalysisException] {
      updateContext.pipelineExecution.startPipeline()
      updateContext.pipelineExecution.awaitCompletion()
    }

    assertAnalysisException(
      ex,
      errorClass = "INVALID_TEMP_OBJ_REFERENCE",
      metadata = Map(
        "objName" -> pv.toString,
        "obj" -> "view",
        "tempObjName" -> "`temp_view`",
        "tempObj" -> "temporary view"
      )
    )
  }

  test("persisted view reads from a non-existent dataset") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"CREATE VIEW myview AS SELECT * FROM nonexistent_view;"
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)

    val ex = intercept[Exception] {
      updateContext.pipelineExecution.startPipeline()
      updateContext.pipelineExecution.awaitCompletion()
    }
    ex match {
      case u: UnresolvedPipelineException =>
        assert(u.directFailures.keySet == Set(fullyQualifiedIdentifier("myview")))
      case _ => fail("Unexpected error", ex)
    }
  }

  test("persisted view reads from a streaming source") {
    val viewName = "mypersistedview"
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""CREATE STREAMING TABLE source AS SELECT * FROM STREAM($externalTable1Ident);
                   |CREATE VIEW $viewName AS SELECT * FROM STREAM(source);""".stripMargin
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)

    val ex = intercept[AnalysisException] {
      updateContext.pipelineExecution.startPipeline()
      updateContext.pipelineExecution.awaitCompletion()
    }

    assertAnalysisException(
      ex,
      errorClass = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_PERSISTED_VIEW"
    )
  }

  test("persisted view reads from an external streaming source") {
    val viewName = "mypersistedview"
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"CREATE VIEW $viewName AS SELECT * FROM STREAM($externalTable1Ident);"
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)

    val ex = intercept[AnalysisException] {
      updateContext.pipelineExecution.startPipeline()
      updateContext.pipelineExecution.awaitCompletion()
    }

    assertAnalysisException(
      ex,
      errorClass = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_PERSISTED_VIEW"
    )
  }

  test("persisted view reads from another persisted view") {
    val session = spark
    import session.implicits._

    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""CREATE VIEW pv3 AS SELECT * FROM pv2;
                   |CREATE VIEW pv2 AS SELECT * FROM pv1;
                   |CREATE VIEW pv1 AS SELECT * FROM range(1, 4);""".stripMargin
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graph = updateContext.pipelineExecution.graphExecution.get.graphForExecution

    verifyPersistedViewMetadata(
      graph = graph,
      viewMetadata = Map(
        fullyQualifiedIdentifier("pv1") -> "SELECT * FROM range(1, 4)",
        fullyQualifiedIdentifier("pv2") -> s"SELECT * FROM pv1",
        fullyQualifiedIdentifier("pv3") -> s"SELECT * FROM pv2"
      )
    )

    checkAnswer(
      spark.sql(buildSelectQuery(fullyQualifiedIdentifier("pv3"))),
      Seq(1L, 2L, 3L).toDF()
    )
  }

  test("persisted view reads from a failed persisted view") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |CREATE VIEW pv2 AS SELECT * FROM pv1;
        |CREATE VIEW pv1 AS SELECT 1 + 1;""".stripMargin
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    // assert that pv1 fails
    assertFlowProgressEvent(
      updateContext.eventBuffer,
      fullyQualifiedIdentifier("pv1"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.ERROR,
      errorChecker = _.getMessage.contains("CREATE_PERMANENT_VIEW_WITHOUT_ALIAS")
    )

    // assert that pv2 is skipped (as it depends on pv1)
    assertFlowProgressEvent(
      updateContext.eventBuffer,
      fullyQualifiedIdentifier("pv2"),
      expectedFlowStatus = FlowStatus.SKIPPED,
      expectedEventLevel = EventLevel.INFO
    )
  }

  test("persisted view reads from MV") {
    val session = spark
    import session.implicits._

    val viewName = "mypersistedview"
    val viewIdentifier = fullyQualifiedIdentifier(viewName)

    val mvIdentifier = fullyQualifiedIdentifier("mymv")

    val source = mvIdentifier.toString
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""CREATE MATERIALIZED VIEW mymv AS SELECT * FROM RANGE(1, 4);
                   |CREATE VIEW $viewName AS SELECT * FROM $source;""".stripMargin
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graph = updateContext.pipelineExecution.graphExecution.get.graphForExecution

    verifyPersistedViewMetadata(
      graph = graph,
      viewMetadata = Map(
        viewIdentifier -> s"SELECT * FROM $source"
      )
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $viewIdentifier"),
      Seq(1L, 2L, 3L).toDF()
    )
  }

  test("persisted view reads from ST") {
    val session = spark
    import session.implicits._

    val viewName = "mypersistedview"
    val viewIdentifier = fullyQualifiedIdentifier(viewName)

    val stIdentifier = fullyQualifiedIdentifier("myst")

    val source = stIdentifier.toString
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""CREATE STREAMING TABLE myst AS SELECT * FROM STREAM($externalTable1Ident);
                   |CREATE VIEW $viewName AS SELECT * FROM $source;""".stripMargin
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graph = updateContext.pipelineExecution.graphExecution.get.graphForExecution

    verifyPersistedViewMetadata(
      graph = graph,
      viewMetadata = Map(
        viewIdentifier -> s"SELECT * FROM $source"
      )
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $viewIdentifier"),
      Seq(0L, 1L, 2L).toDF()
    )
  }

  test("mv reading from a persisted view") {
    val session = spark
    import session.implicits._

    val viewName = "pv"
    val viewIdentifier = fullyQualifiedIdentifier(viewName)

    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |CREATE VIEW $viewName AS SELECT * FROM range(1, 4);
        |CREATE MATERIALIZED VIEW myviewreader AS SELECT * FROM $viewName;""".stripMargin
    )
    val updateContext =
      TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graph = updateContext.pipelineExecution.graphExecution.get.graphForExecution

    verifyPersistedViewMetadata(
      graph = graph,
      viewMetadata = Map(
        viewIdentifier -> "SELECT * FROM range(1, 4)"
      )
    )

    val q = buildSelectQuery(fullyQualifiedIdentifier("myviewreader"))

    checkAnswer(
      spark.sql(q),
      Seq(1L, 2L, 3L).toDF()
    )
    checkAnswer(
      spark.sql(s"SELECT * FROM $viewIdentifier"),
      Seq(1L, 2L, 3L).toDF()
    )

  }

  private def verifyPersistedViewMetadata(
      graph: DataflowGraph,
      viewMetadata: Map[TableIdentifier, String]
  ): Unit = {
    viewMetadata.foreach {
      case (key, value) =>
        val viewOpt = graph.view.get(key)
        assert(viewOpt.isDefined)
        val view = viewOpt.get
        assert(view.isInstanceOf[PersistedView])
        assert(view.sqlText.isDefined && view.sqlText.get == value)
    }
  }

  /** Builds a select query for the given dataset name. */
  private def buildSelectQuery(identifier: TableIdentifier): String = {
    s"SELECT * FROM $identifier"
  }
}
