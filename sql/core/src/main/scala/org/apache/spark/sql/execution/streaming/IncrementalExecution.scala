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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.{InternalOutputModes, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SparkPlanner, UnaryExecNode}
import org.apache.spark.sql.streaming.OutputMode

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 */
class IncrementalExecution private[sql](
    sparkSession: SparkSession,
    logicalPlan: LogicalPlan,
    val outputMode: OutputMode,
    val checkpointLocation: String,
    val currentBatchId: Long)
  extends QueryExecution(sparkSession, logicalPlan) {

  // TODO: make this always part of planning.
  val stateStrategy = sparkSession.sessionState.planner.StatefulAggregationStrategy +:
    sparkSession.sessionState.planner.StreamingRelationStrategy +:
    sparkSession.sessionState.experimentalMethods.extraStrategies

  // Modified planner with stateful operations.
  override def planner: SparkPlanner =
    new SparkPlanner(
      sparkSession.sparkContext,
      sparkSession.sessionState.conf,
      stateStrategy)

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
   */
  private var operatorId = 0

  /** Locates save/restore pairs surrounding aggregation. */
  val state = new Rule[SparkPlan] {

    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case StateStoreSaveExec(keys, None, None,
             UnaryExecNode(agg,
               StateStoreRestoreExec(keys2, None, child))) =>
        val stateId = OperatorStateId(checkpointLocation, operatorId, currentBatchId)
        val returnAllStates = if (outputMode == InternalOutputModes.Complete) true else false
        operatorId += 1

        StateStoreSaveExec(
          keys,
          Some(stateId),
          Some(returnAllStates),
          agg.withNewChildren(
            StateStoreRestoreExec(
              keys,
              Some(stateId),
              child) :: Nil))
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] = state +: super.preparations

  /** No need assert supported, as this check has already been done */
  override def assertSupported(): Unit = { }
}
