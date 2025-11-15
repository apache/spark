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

package org.apache.spark.sql.pipelines.analysis

import scala.collection.mutable.{Map => MutableMap}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.streaming.runtime.StreamingRelation
import org.apache.spark.sql.pipelines.graph.{FlowFunction, FlowFunctionResult, Input, QueryContext, QueryOrigin}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tracker for flow function results.
 * @param flowFunctionResults Mutable map storing the latest FlowFunctionResult per flow function
 */
case class FlowFunctionResultTracker(
    flowFunctionResults: MutableMap[String, FlowFunctionResult]
)

/**
 * Instrumented FlowFunction implementation, used to track flow function results.
 * @param flowName  The name of the flow function being tracked
 * @param flowFunction  The flow function being tracked
 * @param flowFunctionResultTracker The flow function results tracker instance
 */
class InstrumentedFlowFunction(
    flowName: String,
    flowFunction: FlowFunction,
    flowFunctionResultTracker: FlowFunctionResultTracker
)
  extends FlowFunction {
  override def call(
     allInputs: Set[TableIdentifier],
     availableInputs: Seq[Input],
     configuration: Map[String, String],
     queryContext: QueryContext,
     queryOrigin: QueryOrigin
  ): FlowFunctionResult = {
    val flowFunctionResult = flowFunction.call(
      allInputs,
      availableInputs,
      configuration,
      queryContext,
      queryOrigin
    )
    flowFunctionResultTracker.flowFunctionResults.put(flowName, flowFunctionResult)
    flowFunctionResult
  }
}

class InstrumentedTestGraphRegistrationContext(
    spark: SparkSession,
    flowFunctionResultTracker: FlowFunctionResultTracker
)
  extends TestGraphRegistrationContext(spark) {

  def readFlowFunc(
      flowNameForTracking: String,
      tableName: String,
      extraOptions: CaseInsensitiveStringMap
  ): FlowFunction =
    new InstrumentedFlowFunction(
      flowName = flowNameForTracking,
      flowFunction = readFlowFunc(tableName, extraOptions),
      flowFunctionResultTracker = flowFunctionResultTracker
    )

  def readStreamFlowFunc(
      flowNameForTracking: String,
      tableName: String,
      extraOptions: CaseInsensitiveStringMap
  ): FlowFunction =
    new InstrumentedFlowFunction(
      flowName = flowNameForTracking,
      flowFunction = readStreamFlowFunc(tableName, extraOptions),
      flowFunctionResultTracker = flowFunctionResultTracker
    )
}

/**
 * Test suite for verifying propagation of read options during pipelines analysis.
 */
class ReadOptionsPropagationOnAnalysisSuite extends ExecutionTest with SharedSparkSession {
  test("Internal pipeline batch read options are propagated during flow function analysis") {
    val session = spark
    import session.implicits._

    val flowFunctionResultTracker = FlowFunctionResultTracker(MutableMap.empty)

    withTable("a", "b") {
      val graphRegistrationContext =
        new InstrumentedTestGraphRegistrationContext(spark, flowFunctionResultTracker) {
          registerMaterializedView(name = "a", query = dfFlowFunc(Seq(1, 2).toDF("id")))
          registerMaterializedView(
            name = "b",
            query = readFlowFunc(
              flowNameForTracking = "bFlow",
              tableName = "a",
              extraOptions = new CaseInsensitiveStringMap(Map("x" -> "y").asJava)
            )
          )
        }
      val unresolvedGraph = graphRegistrationContext.toDataflowGraph

      val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph, storageRoot)
      updateContext.pipelineExecution.runPipeline()
      updateContext.pipelineExecution.awaitCompletion()

      val bFlow = flowFunctionResultTracker.flowFunctionResults.get("bFlow").get

      // Verify the flow function's analyzed DF logical plan contains specified options.
      assert(bFlow.dataFrame.get.logicalPlan
        .asInstanceOf[SubqueryAlias].child
        .asInstanceOf[LogicalRelation].relation
        .asInstanceOf[HadoopFsRelation].options.get("x").contains("y"))
    }
  }

  test("Internal pipeline stream read options are propagated during flow function analysis") {
    val flowFunctionResultTracker = FlowFunctionResultTracker(MutableMap.empty)

    withTable("spark_catalog.default.a", "b", "c") {
      // Create a regular external table that ST "b" can stream from, then have ST "c" stream from
      // "b".
      spark.range(10).write.saveAsTable("spark_catalog.default.a")

      val graphRegistrationContext =
        new InstrumentedTestGraphRegistrationContext(spark, flowFunctionResultTracker) {
          registerTable(
            name = "b",
            query = Option(
                readStreamFlowFunc(
                  name = "spark_catalog.default.a"
                )
              )
            )
          registerTable(
            name = "c",
            query = Option(
              readStreamFlowFunc(
                flowNameForTracking = "cFlow",
                tableName = "b",
                extraOptions = new CaseInsensitiveStringMap(Map("x" -> "y").asJava)
              )
            )
          )
        }
      val unresolvedGraph = graphRegistrationContext.toDataflowGraph

      val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph, storageRoot)
      updateContext.pipelineExecution.runPipeline()
      updateContext.pipelineExecution.awaitCompletion()

      val cFlow = flowFunctionResultTracker.flowFunctionResults.get("cFlow").get

      // Verify the flow function's analyzed DF logical plan contains specified options.
      assert(cFlow.dataFrame.get.logicalPlan
        .asInstanceOf[SubqueryAlias].child
        .asInstanceOf[StreamingRelation].dataSource.options.get("x").contains("y"))
    }
  }

  test("External pipeline batch read options are propagated during flow function analysis") {
    val flowFunctionResultTracker = FlowFunctionResultTracker(MutableMap.empty)

    withTable("spark_catalog.default.a", "b") {
      // Create regular external table to batch read from with options.
      spark.range(10).write.saveAsTable("spark_catalog.default.a")

      val graphRegistrationContext =
        new InstrumentedTestGraphRegistrationContext(spark, flowFunctionResultTracker) {
          registerMaterializedView(
            name = "b",
            query = readFlowFunc(
              flowNameForTracking = "bFlow",
              tableName = "spark_catalog.default.a",
              extraOptions = new CaseInsensitiveStringMap(Map("x" -> "y").asJava)
            )
          )
        }
      val unresolvedGraph = graphRegistrationContext.toDataflowGraph

      val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph, storageRoot)
      updateContext.pipelineExecution.runPipeline()
      updateContext.pipelineExecution.awaitCompletion()

      val bFlow = flowFunctionResultTracker.flowFunctionResults.get("bFlow").get

      // Verify the flow function's analyzed DF logical plan contains specified options.
      assert(bFlow.dataFrame.get.logicalPlan
        .asInstanceOf[SubqueryAlias].child
        .asInstanceOf[LogicalRelation].relation
        .asInstanceOf[HadoopFsRelation].options.get("x").contains("y"))
    }
  }

  test("External pipeline stream read options are propagated during flow function analysis") {
    val flowFunctionResultTracker = FlowFunctionResultTracker(MutableMap.empty)

    withTable("spark_catalog.default.a", "b") {
      // Create regular external table to stream from with read options.
      spark.range(10).write.saveAsTable("spark_catalog.default.a")

      val graphRegistrationContext =
        new InstrumentedTestGraphRegistrationContext(spark, flowFunctionResultTracker) {
          registerTable(
            name = "b",
            query = Option(
              readStreamFlowFunc(
                flowNameForTracking = "bFlow",
                tableName = "spark_catalog.default.a",
                extraOptions = new CaseInsensitiveStringMap(Map("x" -> "y").asJava)
              )
            )
          )
        }
      val unresolvedGraph = graphRegistrationContext.toDataflowGraph

      val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph, storageRoot)
      updateContext.pipelineExecution.runPipeline()
      updateContext.pipelineExecution.awaitCompletion()

      val bFlow = flowFunctionResultTracker.flowFunctionResults.get("bFlow").get

      // Verify the flow function's analyzed DF logical plan contains specified options.
      assert(bFlow.dataFrame.get.logicalPlan
        .asInstanceOf[SubqueryAlias].child
        .asInstanceOf[StreamingRelation].dataSource.options.get("x").contains("y"))
    }
  }
}
