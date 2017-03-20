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
import org.apache.spark.sql.{DataFrame, Encoder, ForeachWriter}
import org.apache.spark.sql.catalyst.encoders.encoderFor

/**
 * A [[Sink]] that forwards all data into [[ForeachWriter]] according to the contract defined by
 * [[ForeachWriter]].
 *
 * @param writer The [[ForeachWriter]] to process all data.
 * @tparam T The expected type of the sink.
 */
class ForeachSink[T : Encoder](writer: ForeachWriter[T]) extends Sink with Serializable {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    // This logic should've been as simple as:
    // ```
    //   data.as[T].foreachPartition { iter => ... }
    // ```
    //
    // Unfortunately, doing that would just break the incremental planing. The reason is,
    // `Dataset.foreachPartition()` would further call `Dataset.rdd()`, but `Dataset.rdd()` will
    // create a new plan. Because StreamExecution uses the existing plan to collect metrics and
    // update watermark, we should never create a new plan. Otherwise, metrics and watermark are
    // updated in the new plan, and StreamExecution cannot retrieval them.
    //
    // Hence, we need to manually convert internal rows to objects using encoder.
    val encoder = encoderFor[T].resolveAndBind(
      data.logicalPlan.output,
      data.sparkSession.sessionState.analyzer)
    data.queryExecution.toRdd.foreachPartition { iter =>
      if (writer.open(TaskContext.getPartitionId(), batchId)) {
        try {
          while (iter.hasNext) {
            writer.process(encoder.fromRow(iter.next()))
          }
        } catch {
          case e: Throwable =>
            writer.close(e)
            throw e
        }
        writer.close(null)
      } else {
        writer.close(null)
      }
    }
  }
}
