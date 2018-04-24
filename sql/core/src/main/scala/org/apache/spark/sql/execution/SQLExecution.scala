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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart}

object SQLExecution {

  val EXECUTION_ID_KEY = "spark.sql.execution.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that
   * we can connect them with an execution.
   */
  def withNewExecutionId[T](
      sparkSession: SparkSession,
      queryExecution: QueryExecution)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY)
    if (oldExecutionId == null) {
      val executionId = SQLExecution.nextExecutionId
      sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
      executionIdToQueryExecution.put(executionId, queryExecution)
      val r = try {
        // sparkContext.getCallSite() would first try to pick up any call site that was previously
        // set, then fall back to Utils.getCallSite(); call Utils.getCallSite() directly on
        // streaming queries would give us call site like "run at <unknown>:0"
        val callSite = sparkSession.sparkContext.getCallSite()

        sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionStart(
          executionId, callSite.shortForm, callSite.longForm, queryExecution.toString,
          SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan), System.currentTimeMillis()))
        try {
          body
        } finally {
          sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionEnd(
            executionId, System.currentTimeMillis()))
        }
      } finally {
        executionIdToQueryExecution.remove(executionId)
        sc.setLocalProperty(EXECUTION_ID_KEY, null)
      }
      r
    } else {
      // Don't support nested `withNewExecutionId`. This is an example of the nested
      // `withNewExecutionId`:
      //
      // class DataFrame {
      //   def foo: T = withNewExecutionId { something.createNewDataFrame().collect() }
      // }
      //
      // Note: `collect` will call withNewExecutionId
      // In this case, only the "executedPlan" for "collect" will be executed. The "executedPlan"
      // for the outer DataFrame won't be executed. So it's meaningless to create a new Execution
      // for the outer DataFrame. Even if we track it, since its "executedPlan" doesn't run,
      // all accumulator metrics will be 0. It will confuse people if we show them in Web UI.
      //
      // A real case is the `DataFrame.count` method.
      throw new IllegalArgumentException(s"$EXECUTION_ID_KEY is already set")
    }
  }

  /**
   * Wrap an action with a known executionId. When running a different action in a different
   * thread from the original one, this method can be used to connect the Spark jobs in this action
   * with the known executionId, e.g., `BroadcastExchangeExec.relationFuture`.
   */
  def withExecutionId[T](sc: SparkContext, executionId: String)(body: => T): T = {
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    try {
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
      body
    } finally {
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
    }
  }
}
