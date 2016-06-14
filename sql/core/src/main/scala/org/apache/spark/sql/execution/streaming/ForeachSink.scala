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

/**
 * A [[Sink]] that forwards all data into [[ForeachWriter]] according to the contract defined by
 * [[ForeachWriter]].
 *
 * @param writer The [[ForeachWriter]] to process all data.
 * @tparam T The expected type of the sink.
 */
class ForeachSink[T : Encoder](writer: ForeachWriter[T]) extends Sink with Serializable {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    data.as[T].foreachPartition { iter =>
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
