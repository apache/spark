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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.SparkPlan

/** Used to identify the state store for a given operator. */
case class OperatorStateId(
    checkpointLocation: String,
    operatorId: Long,
    batchId: Long)

/**
 * An operator that saves or restores state from the [[StateStore]].  The [[OperatorStateId]] should
 * be filled in by `prepareForExecution` in [[IncrementalExecution]].
 */
trait StatefulOperator extends SparkPlan {
  def stateId: Option[OperatorStateId]

  protected def getStateId: OperatorStateId = attachTree(this) {
    stateId.getOrElse {
      throw new IllegalStateException("State location not present for execution")
    }
  }
}

/**
 * For each input tuple, the key is calculated and the value from the [[StateStore]] is added
 * to the stream (in addition to the input tuple) if present.
 */
case class StateStoreRestoreExec(
    keyExpressions: Seq[Attribute],
    stateId: Option[OperatorStateId],
    child: SparkPlan)
  extends execution.UnaryExecNode with StatefulOperator {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithStateStore(
      getStateId.checkpointLocation,
      operatorId = getStateId.operatorId,
      storeVersion = getStateId.batchId,
      keyExpressions.toStructType,
      child.output.toStructType,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        iter.flatMap { row =>
          val key = getKey(row)
          val savedState = store.get(key)
          numOutputRows += 1
          row +: savedState.toSeq
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/**
 * For each input tuple, the key is calculated and the tuple is `put` into the [[StateStore]].
 */
case class StateStoreSaveExec(
    keyExpressions: Seq[Attribute],
    stateId: Option[OperatorStateId],
    returnAllStates: Option[Boolean],
    child: SparkPlan)
  extends execution.UnaryExecNode with StatefulOperator {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numTotalStateRows" -> SQLMetrics.createMetric(sparkContext, "number of total state rows"),
    "numUpdatedStateRows" -> SQLMetrics.createMetric(sparkContext, "number of updated state rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    assert(returnAllStates.nonEmpty,
      "Incorrect planning in IncrementalExecution, returnAllStates have not been set")
    val saveAndReturnFunc = if (returnAllStates.get) saveAndReturnAll _ else saveAndReturnUpdated _
    child.execute().mapPartitionsWithStateStore(
      getStateId.checkpointLocation,
      operatorId = getStateId.operatorId,
      storeVersion = getStateId.batchId,
      keyExpressions.toStructType,
      child.output.toStructType,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)
    )(saveAndReturnFunc)
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  /**
   * Save all the rows to the state store, and return all the rows in the state store.
   * Note that this returns an iterator that pipelines the saving to store with downstream
   * processing.
   */
  private def saveAndReturnUpdated(
      store: StateStore,
      iter: Iterator[InternalRow]): Iterator[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numTotalStateRows = longMetric("numTotalStateRows")
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")

    new Iterator[InternalRow] {
      private[this] val baseIterator = iter
      private[this] val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)

      override def hasNext: Boolean = {
        if (!baseIterator.hasNext) {
          store.commit()
          numTotalStateRows += store.numKeys()
          false
        } else {
          true
        }
      }

      override def next(): InternalRow = {
        val row = baseIterator.next().asInstanceOf[UnsafeRow]
        val key = getKey(row)
        store.put(key.copy(), row.copy())
        numOutputRows += 1
        numUpdatedStateRows += 1
        row
      }
    }
  }

  /**
   * Save all the rows to the state store, and return all the rows in the state store.
   * Note that the saving to store is blocking; only after all the rows have been saved
   * is the iterator on the update store data is generated.
   */
  private def saveAndReturnAll(
      store: StateStore,
      iter: Iterator[InternalRow]): Iterator[InternalRow] = {
    val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
    val numOutputRows = longMetric("numOutputRows")
    val numTotalStateRows = longMetric("numTotalStateRows")
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")

    while (iter.hasNext) {
      val row = iter.next().asInstanceOf[UnsafeRow]
      val key = getKey(row)
      store.put(key.copy(), row.copy())
      numUpdatedStateRows += 1
    }
    store.commit()
    numTotalStateRows += store.numKeys()
    store.iterator().map { case (k, v) =>
      numOutputRows += 1
      v.asInstanceOf[InternalRow]
    }
  }
}
