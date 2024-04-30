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

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.MDC
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * The RDD writing to a sink in continuous processing.
 *
 * Within each task, we repeatedly call prev.compute(). Each resulting iterator contains the data
 * to be written for one epoch, which we commit and forward to the driver.
 *
 * We keep repeating prev.compute() and writing new epochs until the query is shut down.
 */
class ContinuousWriteRDD(var prev: RDD[InternalRow], writerFactory: StreamingDataWriterFactory,
    customMetrics: Map[String, SQLMetric]) extends RDD[Unit](prev) {

  override val partitioner = prev.partitioner

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Unit] = {
    val epochCoordinator = EpochCoordinatorRef.get(
      context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
      SparkEnv.get)
    EpochTracker.initializeCurrentEpoch(
      context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong)
    while (!context.isInterrupted() && !context.isCompleted()) {
      var dataWriter: DataWriter[InternalRow] = null
      // write the data and commit this writer.
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        try {
          val dataIterator = prev.compute(split, context)
          dataWriter = writerFactory.createWriter(
            context.partitionId(),
            context.taskAttemptId(),
            EpochTracker.getCurrentEpoch.get)
          var count = 0L
          while (dataIterator.hasNext) {
            if (count % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
              CustomMetrics.updateMetrics(
                dataWriter.currentMetricsValues.toImmutableArraySeq, customMetrics)
            }
            count += 1
            dataWriter.write(dataIterator.next())
          }
          CustomMetrics.updateMetrics(
            dataWriter.currentMetricsValues.toImmutableArraySeq, customMetrics)
          logInfo(log"Writer for partition ${MDC(PARTITION_ID, context.partitionId())} " +
            log"in epoch ${MDC(EPOCH, EpochTracker.getCurrentEpoch.get)} is committing.")
          val msg = dataWriter.commit()
          epochCoordinator.send(
            CommitPartitionEpoch(
              context.partitionId(),
              EpochTracker.getCurrentEpoch.get,
              msg)
          )
          logInfo(log"Writer for partition ${MDC(PARTITION_ID, context.partitionId())} " +
            log"in epoch ${MDC(EPOCH, EpochTracker.getCurrentEpoch.get)} committed.")
          EpochTracker.incrementCurrentEpoch()
        } catch {
          case _: InterruptedException =>
          // Continuous shutdown always involves an interrupt. Just finish the task.
        }
      })(catchBlock = {
        // If there is an error, abort this writer. We enter this callback in the middle of
        // rethrowing an exception, so compute() will stop executing at this point.
        logError(s"Writer for partition ${context.partitionId()} is aborting.")
        if (dataWriter != null) dataWriter.abort()
        logError(s"Writer for partition ${context.partitionId()} aborted.")
      }, finallyBlock = {
        if (dataWriter != null) dataWriter.close()
      })
    }

    Iterator()
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}
