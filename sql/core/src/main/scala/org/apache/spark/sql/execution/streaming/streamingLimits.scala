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
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.execution.{LimitExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.streaming.state.StateStoreOps
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, NullType, StructField, StructType}
import org.apache.spark.util.{CompletionIterator, NextIterator}

/**
 * A physical operator for executing a streaming limit, which makes sure no more than streamLimit
 * rows are returned. This physical operator is only meant for logical limit operations that
 * will get a input stream of rows that are effectively appends. For example,
 * - limit on any query in append mode
 * - limit before the aggregation in a streaming aggregation query complete mode
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


/**
 * A physical operator for executing limits locally on each partition. The main difference from
 * LocalLimitExec is that this will fully consume `child` plan's iterators to ensure that any
 * stateful operation within `child` commits all the state changes (many stateful operations
 * commit state changes only after the iterator is consumed).
 */
case class StreamingLocalLimitExec(limit: Int, child: SparkPlan)
  extends LimitExec {

  override def doExecute(): RDD[InternalRow] = child.execute().mapPartitions { iter =>

    var generatedCount = 0

    new NextIterator[InternalRow]() {
      override protected def getNext(): InternalRow = {
        if (generatedCount < limit && iter.hasNext) {
          generatedCount += 1
          iter.next()
        } else {
          finished = true
          null
        }
      }

      override protected def close(): Unit = {
        while (iter.hasNext) iter.next() // consume the iterator completely
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def output: Seq[Attribute] = child.output
}
