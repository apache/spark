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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, ForeachWriter}
import org.apache.spark.sql.catalyst.plans.logical.CatalystSerde
import org.apache.spark.sql.execution.SQLExecution

/**
 * A [[Sink]] that forwards all data into [[ForeachWriter]] according to the contract defined by
 * [[ForeachWriter]].
 *
 * @param writer The [[ForeachWriter]] to process all data.
 * @tparam T The expected type of the sink.
 */
class ForeachSink[T : Encoder](writer: ForeachWriter[T]) extends Sink with Serializable {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    // TODO: Refine this method when SPARK-16264 is resolved; see comments below.

    // This logic should've been as simple as:
    // ```
    //   data.as[T].foreachPartition { iter => ... }
    // ```
    //
    // Unfortunately, doing that would just break the incremental planing. The reason is,
    // `Dataset.foreachPartition()` would further call `Dataset.rdd()`, but `Dataset.rdd()` just
    // does not support `IncrementalExecution`.
    //
    // So as a provisional fix, below we've made a special version of `Dataset` with its `rdd()`
    // method supporting incremental planning. But in the long run, we should generally make newly
    // created Datasets use `IncrementalExecution` where necessary (which is SPARK-16264 tries to
    // resolve).
    //
    // Another thing worthy noting is, by introducing this `incrementalExecution`, we've also
    // introduced one extra round of physical planning; refer to SPARK-16545 for details. Again,
    // let's revisit this when SPARK-16264 is resolved.

    val datasetWithIncrementalExecution =
      new Dataset(data.sparkSession, data.logicalPlan, implicitly[Encoder[T]]) {

        private var incrementalExecution: IncrementalExecution = _

        override lazy val rdd: RDD[T] = {
          val objectType = exprEnc.deserializer.dataType
          val deserialized = CatalystSerde.deserialize[T](logicalPlan)

          // was originally: sparkSession.sessionState.executePlan(deserialized) ...
          incrementalExecution = new IncrementalExecution(
            this.sparkSession,
            deserialized,
            data.queryExecution.asInstanceOf[IncrementalExecution].outputMode,
            data.queryExecution.asInstanceOf[IncrementalExecution].checkpointLocation,
            data.queryExecution.asInstanceOf[IncrementalExecution].currentBatchId)
          incrementalExecution.toRdd.mapPartitions { rows =>
            rows.map(_.get(0, objectType))
          }.asInstanceOf[RDD[T]]
        }

        /**
         * This function's original implementation was:
         * ```
         *   def foreachPartition(f: Iterator[T] => Unit): Unit = withNewExecutionId {
         *    rdd.foreachPartition(f)
         *  }
         * ```
         *
         * Below we overwrite it and provide `withNewExecutionId` with `this.incrementExecution`
         * rather than with `this.queryExecution`; doing this would save one round of unnecessary
         * physical planning. Refer to SPARK-16545 for details.
         */
        override def foreachPartition(f: Iterator[T] => Unit): Unit = {
          rdd // call rdd first to set incrementalExecution properly
          SQLExecution.withNewExecutionId(this.sparkSession, this.incrementalExecution) {
            rdd.foreachPartition(f)
          }
        }
      }

    datasetWithIncrementalExecution.foreachPartition { iter =>
      if (writer.open(TaskContext.getPartitionId(), batchId)) {
        var isFailed = false
        try {
          while (iter.hasNext) {
            writer.process(iter.next())
          }
        } catch {
          case e: Throwable =>
            isFailed = true
            writer.close(e)
        }
        if (!isFailed) {
          writer.close(null)
        }
      } else {
        writer.close(null)
      }
    }
  }
}
