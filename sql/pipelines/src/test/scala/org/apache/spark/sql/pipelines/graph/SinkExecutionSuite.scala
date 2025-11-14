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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.test.SharedSparkSession

class SinkExecutionSuite extends ExecutionTest with SharedSparkSession {
  def createDataflowGraph(
      inputs: DataFrame,
      sinkName: String,
      flowName: String,
      format: String,
      sinkOptions: Map[String, String] = Map.empty
  ): DataflowGraph = {
    val registrationContext = new TestGraphRegistrationContext(spark) {
      registerTemporaryView("a", query = dfFlowFunc(inputs))
      registerSink(sinkName, format, sinkOptions)
      registerFlow(sinkName, flowName, query = readStreamFlowFunc("a"))
    }
    registrationContext.toDataflowGraph
  }

  test("writing to external sink - memory sink") {
    val session = spark
    import session.implicits._

    val ints = MemoryStream[Int]
    ints.addData(1, 2, 3, 4)

    val unresolvedGraph =
      createDataflowGraph(ints.toDF(), "sink_a", "flow_to_sink_a", "memory")
    val updateContext = TestPipelineUpdateContext(
      spark,
      unresolvedGraph,
      storageRoot,
      failOnErrorEvent = true
    )
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    verifyCheckpointLocation(
      storageRoot,
      updateContext.pipelineExecution.graphExecution.get,
      TableIdentifier("sink_a"),
      TableIdentifier("flow_to_sink_a")
    )

    checkAnswer(spark.sql("SELECT * FROM flow_to_sink_a"), Seq(1, 2, 3, 4).toDF().collect().toSeq)
  }

  test("writing to external sink - parquet sink with path") {
    val session = spark
    import session.implicits._

    withTempDir { externalDeltaPath =>
      val ints = MemoryStream[Int]
      ints.addData(1, 2, 3, 4)
      val unresolvedGraph = createDataflowGraph(
        ints.toDF(),
        "parquet_sink",
        "flow_to_parquet_sink",
        "parquet",
        Map(
          "path" -> externalDeltaPath.getPath
        )
      )

      val updateContext = TestPipelineUpdateContext(
        spark,
        unresolvedGraph,
        storageRoot
      )
      updateContext.pipelineExecution.startPipeline()
      updateContext.pipelineExecution.awaitCompletion()

      verifyCheckpointLocation(
        storageRoot,
        updateContext.pipelineExecution.graphExecution.get,
        TableIdentifier("parquet_sink"),
        TableIdentifier("flow_to_parquet_sink")
      )

      checkAnswer(
        spark.read.format("parquet").load(externalDeltaPath.getPath),
        Seq(1, 2, 3, 4).toDF().collect().toSeq
      )
    }
  }

  def verifyCheckpointLocation(
      rootDirectory: String,
      graphExecution: GraphExecution,
      sinkIdentifier: TableIdentifier,
      flowIdentifier: TableIdentifier): Unit = {
    val expectedCheckpointLocation = new Path(
      rootDirectory + s"/_checkpoints/${sinkIdentifier.table}/${flowIdentifier.table}/0"
    )
    val streamingQuery = graphExecution
      .flowExecutions(flowIdentifier)
      .asInstanceOf[StreamingFlowExecution]
      .getStreamingQuery

    val actualCheckpointLocation = new Path(getCheckpointPath(streamingQuery))

    assert(actualCheckpointLocation == expectedCheckpointLocation)
  }

  private def getCheckpointPath(q: StreamingQuery): String =
    q.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot
}
