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
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.TaskContext


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
    stateId: Option[OperatorStateId] = None,
    outputMode: Option[OutputMode] = None,
    eventTimeWatermark: Option[Long] = None,
    child: SparkPlan)
  extends execution.UnaryExecNode with StatefulOperator {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numTotalStateRows" -> SQLMetrics.createMetric(sparkContext, "number of total state rows"),
    "numUpdatedStateRows" -> SQLMetrics.createMetric(sparkContext, "number of updated state rows"))

  /** Generate a predicate that matches data older than the watermark */
  private lazy val watermarkPredicate: Option[Predicate] = {
    val optionalWatermarkAttribute =
      keyExpressions.find(_.metadata.contains(EventTimeWatermark.delayKey))

    optionalWatermarkAttribute.map { watermarkAttribute =>
      // If we are evicting based on a window, use the end of the window.  Otherwise just
      // use the attribute itself.
      val evictionExpression =
        if (watermarkAttribute.dataType.isInstanceOf[StructType]) {
          LessThanOrEqual(
            GetStructField(watermarkAttribute, 1),
            Literal(eventTimeWatermark.get * 1000))
        } else {
          LessThanOrEqual(
            watermarkAttribute,
            Literal(eventTimeWatermark.get * 1000))
        }

      logInfo(s"Filtering state store on: $evictionExpression")
      newPredicate(evictionExpression, keyExpressions)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    assert(outputMode.nonEmpty,
      "Incorrect planning in IncrementalExecution, outputMode has not been set")

    child.execute().mapPartitionsWithStateStore(
      getStateId.checkpointLocation,
      operatorId = getStateId.operatorId,
      storeVersion = getStateId.batchId,
      keyExpressions.toStructType,
      child.output.toStructType,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { (store, iter) =>
        val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        val numOutputRows = longMetric("numOutputRows")
        val numTotalStateRows = longMetric("numTotalStateRows")
        val numUpdatedStateRows = longMetric("numUpdatedStateRows")

        // Abort the state store in case of error
        TaskContext.get().addTaskCompletionListener(_ => {
          if (!store.hasCommitted) {
            store.abort()
          }
        })

        outputMode match {
          // Update and output all rows in the StateStore.
          case Some(Complete) =>
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

          // Update and output only rows being evicted from the StateStore
          case Some(Append) =>
            while (iter.hasNext) {
              val row = iter.next().asInstanceOf[UnsafeRow]
              val key = getKey(row)
              store.put(key.copy(), row.copy())
              numUpdatedStateRows += 1
            }

            // Assumption: Append mode can be done only when watermark has been specified
            store.remove(watermarkPredicate.get.eval)
            store.commit()

            numTotalStateRows += store.numKeys()
            store.updates().filter(_.isInstanceOf[ValueRemoved]).map { removed =>
              numOutputRows += 1
              removed.value.asInstanceOf[InternalRow]
            }

          // Update and output modified rows from the StateStore.
          case Some(Update) =>

            new Iterator[InternalRow] {

              // Filter late date using watermark if specified
              private[this] val baseIterator = watermarkPredicate match {
                case Some(predicate) => iter.filter((row: InternalRow) => !predicate.eval(row))
                case None => iter
              }

              override def hasNext: Boolean = {
                if (!baseIterator.hasNext) {
                  // Remove old aggregates if watermark specified
                  if (watermarkPredicate.nonEmpty) store.remove(watermarkPredicate.get.eval)
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

          case _ => throw new UnsupportedOperationException(s"Invalid output mode: $outputMode")
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}
