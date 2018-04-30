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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.TaskContext

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset

/**
 * The data component of [[ContinuousQueuedDataReader]]. Pushes (row, offset) to the queue when
 * a new row arrives to the [[DataReader]].
 */
class DataReaderThread(
    reader: DataReader[UnsafeRow],
    queue: BlockingQueue[(UnsafeRow, PartitionOffset)],
    context: TaskContext,
    failedFlag: AtomicBoolean)
  extends Thread(
    s"continuous-reader--${context.partitionId()}--" +
    s"${context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)}") {
  private[continuous] var failureReason: Throwable = _

  override def run(): Unit = {
    TaskContext.setTaskContext(context)
    val baseReader = ContinuousDataSourceRDD.getBaseReader(reader)
    try {
      while (!context.isInterrupted && !context.isCompleted()) {
        if (!reader.next()) {
          // Check again, since reader.next() might have blocked through an incoming interrupt.
          if (!context.isInterrupted && !context.isCompleted()) {
            throw new IllegalStateException(
              "Continuous reader reported no elements! Reader should have blocked waiting.")
          } else {
            return
          }
        }

        queue.put((reader.get().copy(), baseReader.getOffset))
      }
    } catch {
      case _: InterruptedException if context.isInterrupted() =>
        // Continuous shutdown always involves an interrupt; do nothing and shut down quietly.

      case t: Throwable =>
        failureReason = t
        failedFlag.set(true)
        // Don't rethrow the exception in this thread. It's not needed, and the default Spark
        // exception handler will kill the executor.
    } finally {
      reader.close()
    }
  }
}
