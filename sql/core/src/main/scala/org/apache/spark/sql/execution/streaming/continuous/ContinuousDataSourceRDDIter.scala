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

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, RowToUnsafeDataReader}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.SystemClock

class ContinuousDataSourceRDD(
    sc: SparkContext,
    @transient private val readTasks: java.util.List[ReadTask[UnsafeRow]])
  extends RDD[UnsafeRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    readTasks.asScala.zipWithIndex.map {
      case (readTask, index) => new DataSourceRDDPartition(index, readTask)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    val reader = split.asInstanceOf[DataSourceRDDPartition].readTask.createDataReader()

    // TODO: capacity option
    val queue = new ArrayBlockingQueue[(UnsafeRow, PartitionOffset)](1024)

    val epochPollThread = new EpochPollThread(queue, context)
    epochPollThread.setDaemon(true)
    epochPollThread.start()

    val dataReaderThread = new DataReaderThread(reader, queue, context)
    dataReaderThread.setDaemon(true)
    dataReaderThread.start()

    context.addTaskCompletionListener(_ => {
      reader.close()
      dataReaderThread.interrupt()
      epochPollThread.interrupt()
    })

    val epochEndpoint = EpochCoordinatorRef.get(
      context.getLocalProperty(StreamExecution.QUERY_ID_KEY), SparkEnv.get)
    new Iterator[UnsafeRow] {
      private var currentRow: UnsafeRow = _
      private var currentOffset: PartitionOffset = _

      override def hasNext(): Boolean = {
        val newTuple = queue.take()
        val newOffset = newTuple._2
        currentRow = newTuple._1
        if (currentRow == null) {
          epochEndpoint.send(ReportPartitionOffset(
            context.partitionId(),
            newOffset.asInstanceOf[EpochPackedPartitionOffset].epoch,
            currentOffset))
          false
        } else {
          currentOffset = newOffset
          true
        }
      }

      override def next(): UnsafeRow = {
        val r = currentRow
        currentRow = null
        r
      }
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition].readTask.preferredLocations()
  }
}

case class EpochPackedPartitionOffset(epoch: Long) extends PartitionOffset

class EpochPollThread(
    queue: BlockingQueue[(UnsafeRow, PartitionOffset)],
    context: TaskContext)
  extends Thread with Logging {
  private val epochEndpoint = EpochCoordinatorRef.get(
    context.getLocalProperty(StreamExecution.QUERY_ID_KEY), SparkEnv.get)
  private var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

  override def run(): Unit = {
    // TODO parameterize
    try {
      ProcessingTimeExecutor(ProcessingTime(100), new SystemClock())
        .execute { () =>
            val newEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch())
            for (i <- currentEpoch to newEpoch - 1) {
              queue.put((null, EpochPackedPartitionOffset(i + 1)))
              logDebug(s"Sent marker to start epoch ${i + 1}")
            }
            currentEpoch = newEpoch
            true
        }
    } catch {
      case (_: InterruptedException | _: SparkException) if context.isInterrupted() =>
        // Continuous shutdown might interrupt us, or it might clean up the endpoint before
        // interrupting us. Unfortunately, a missing endpoint just throws a generic SparkException.
        // In either case, as long as the context shows interrupted, we can safely clean shutdown.
        return
    }
  }
}

class DataReaderThread(
    reader: DataReader[UnsafeRow],
    queue: BlockingQueue[(UnsafeRow, PartitionOffset)],
    context: TaskContext) extends Thread {
  override def run(): Unit = {
    val baseReader = reader match {
      case r: ContinuousDataReader[UnsafeRow] => r
      case wrapped: RowToUnsafeDataReader =>
        wrapped.rowReader.asInstanceOf[ContinuousDataReader[Row]]
      case _ =>
        throw new IllegalStateException(s"Unknown continuous reader type ${reader.getClass}")
    }
    try {
      while (!context.isInterrupted && !context.isCompleted()) {
        if (!reader.next()) {
          throw new IllegalStateException(
            "Continuous reader reported no remaining elements! Reader should have blocked waiting.")
        }

        queue.put((reader.get(), baseReader.getOffset))
      }
    } catch {
      case _: InterruptedException if context.isInterrupted() =>
        // Continuous shutdown always involves an interrupt; shut down quietly.
        return
    }
  }
}
