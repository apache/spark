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

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.streaming.state.StateStoreOps
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, NullType, StructField, StructType}
import org.apache.spark.util.CompletionIterator

/**
 * A physical operator for executing a streaming limit, which makes sure no more than streamLimit
 * rows are returned. This operator is meant for streams in Append mode only.
 */
case class StreamingGlobalLimitExec(
    streamLimit: Long,
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    outputMode: Option[OutputMode] = None)
  extends UnaryExecNode with StateStoreWriter {

  private val keySchema = StructType(Array(StructField("key", NullType)))
  private val valueSchema = StructType(Array(StructField("value", LongType)))

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    assert(outputMode.isDefined && outputMode.get == InternalOutputModes.Append,
      "StreamingGlobalLimitExec is only valid for streams in Append output mode")

    child.execute().mapPartitionsWithStateStore(
        getStateInfo,
        keySchema,
        valueSchema,
        indexOrdinal = None,
        sqlContext.sessionState,
        Some(sqlContext.streams.stateStoreCoordinator)) { (store, iter) =>
      val key = UnsafeProjection.create(keySchema)(new GenericInternalRow(Array[Any](null)))
      val numOutputRows = longMetric("numOutputRows")
      val numUpdatedStateRows = longMetric("numUpdatedStateRows")
      val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")
      val updatesStartTimeNs = System.nanoTime

      val preBatchRowCount: Long = Option(store.get(key)).map(_.getLong(0)).getOrElse(0L)
      var cumulativeRowCount = preBatchRowCount

      val result = iter.filter { r =>
        val x = cumulativeRowCount < streamLimit
        if (x) {
          cumulativeRowCount += 1
        }
        x
      }

      CompletionIterator[InternalRow, Iterator[InternalRow]](result, {
        if (cumulativeRowCount > preBatchRowCount) {
          numUpdatedStateRows += 1
          numOutputRows += cumulativeRowCount - preBatchRowCount
          store.put(key, getValueRow(cumulativeRowCount))
        }
        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
        commitTimeMs += timeTakenMs { store.commit() }
        setStoreMetrics(store)
      })
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = AllTuples :: Nil

  private def getValueRow(value: Long): UnsafeRow = {
    UnsafeProjection.create(valueSchema)(new GenericInternalRow(Array[Any](value)))
  }
}
