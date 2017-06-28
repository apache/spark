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

  private val IGNORE_NESTED_EXECUTION_ID = "spark.sql.execution.ignoreNestedExecutionId"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  private val testing = sys.props.contains("spark.testing")

  private[sql] def checkSQLExecutionId(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    val isNestedExecution = sc.getLocalProperty(IGNORE_NESTED_EXECUTION_ID) != null
    val hasExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY) != null
    // only throw an exception during tests. a missing execution ID should not fail a job.
    if (testing && !isNestedExecution && !hasExecutionId) {
      // Attention testers: when a test fails with this exception, it means that the action that
      // started execution of a query didn't call withNewExecutionId. The execution ID should be
      // set by calling withNewExecutionId in the action that begins execution, like
      // Dataset.collect or DataFrameWriter.insertInto.
      throw new IllegalStateException("Execution ID should be set")
    }
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
      try {
        // sparkContext.getCallSite() would first try to pick up any call site that was previously
        // set, then fall back to Utils.getCallSite(); call Utils.getCallSite() directly on
        // streaming queries would give us call site like "run at <unknown>:0"
        val callSite = sparkSession.sparkContext.getCallSite()

        sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionStart(
          executionId, callSite.shortForm, callSite.longForm, queryExecution.toString,
          SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan), queryExecution.sqlText,
          System.currentTimeMillis()))
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
    } else if (sc.getLocalProperty(IGNORE_NESTED_EXECUTION_ID) != null) {
      // If `IGNORE_NESTED_EXECUTION_ID` is set, just ignore the execution id while evaluating the
      // `body`, so that Spark jobs issued in the `body` won't be tracked.
      try {
        sc.setLocalProperty(EXECUTION_ID_KEY, null)
        body
      } finally {
        sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
      }
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
      throw new IllegalArgumentException(s"$EXECUTION_ID_KEY is already set, please wrap your " +
        "action with SQLExecution.ignoreNestedExecutionId if you don't want to track the Spark " +
        "jobs issued by the nested execution.")
    }
  }

  /**
   * Wrap an action with a known executionId. When running a different action in a different
   * thread from the original one, this method can be used to connect the Spark jobs in this action
   * with the known executionId, e.g., `BroadcastHashJoin.broadcastFuture`.
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

  /**
   * Wrap an action which may have nested execution id. This method can be used to run an execution
   * inside another execution, e.g., `CacheTableCommand` need to call `Dataset.collect`. Note that,
   * all Spark jobs issued in the body won't be tracked in UI.
   */
  def ignoreNestedExecutionId[T](sparkSession: SparkSession)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val allowNestedPreviousValue = sc.getLocalProperty(IGNORE_NESTED_EXECUTION_ID)
    try {
      sc.setLocalProperty(IGNORE_NESTED_EXECUTION_ID, "true")
      body
    } finally {
      sc.setLocalProperty(IGNORE_NESTED_EXECUTION_ID, allowNestedPreviousValue)
    }
  }
}
