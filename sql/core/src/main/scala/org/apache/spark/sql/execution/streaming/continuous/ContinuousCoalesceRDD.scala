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

import java.util.UUID

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.continuous.shuffle._
import org.apache.spark.util.ThreadUtils

case class ContinuousCoalesceRDDPartition(
    index: Int,
    endpointName: String,
    queueSize: Int,
    numShuffleWriters: Int,
    epochIntervalMs: Long)
  extends Partition {
  // Initialized only on the executor, and only once even as we call compute() multiple times.
  lazy val (reader: ContinuousShuffleReader, endpoint) = {
    val env = SparkEnv.get.rpcEnv
    val receiver = new RPCContinuousShuffleReader(
      queueSize, numShuffleWriters, epochIntervalMs, env)
    val endpoint = env.setupEndpoint(endpointName, receiver)

    TaskContext.get().addTaskCompletionListener[Unit] { ctx =>
      env.stop(endpoint)
    }
    (receiver, endpoint)
  }
  // This flag will be flipped on the executors to indicate that the threads processing
  // partitions of the write-side RDD have been started. These will run indefinitely
  // asynchronously as epochs of the coalesce RDD complete on the read side.
  private[continuous] var writersInitialized: Boolean = false
}

/**
 * RDD for continuous coalescing. Asynchronously writes all partitions of `prev` into a local
 * continuous shuffle, and then reads them in the task thread using `reader`.
 */
class ContinuousCoalesceRDD(
    context: SparkContext,
    numPartitions: Int,
    readerQueueSize: Int,
    epochIntervalMs: Long,
    prev: RDD[InternalRow])
  extends RDD[InternalRow](context, Nil) {

  // When we support more than 1 target partition, we'll need to figure out how to pass in the
  // required partitioner.
  private val outputPartitioner = new HashPartitioner(1)

  private val readerEndpointNames = (0 until numPartitions).map { i =>
    s"ContinuousCoalesceRDD-part$i-${UUID.randomUUID()}"
  }

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { partIndex =>
      ContinuousCoalesceRDDPartition(
        partIndex,
        readerEndpointNames(partIndex),
        readerQueueSize,
        prev.getNumPartitions,
        epochIntervalMs)
    }.toArray
  }

  private lazy val threadPool = ThreadUtils.newDaemonFixedThreadPool(
    prev.getNumPartitions,
    this.name)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val part = split.asInstanceOf[ContinuousCoalesceRDDPartition]

    if (!part.writersInitialized) {
      val rpcEnv = SparkEnv.get.rpcEnv

      // trigger lazy initialization
      part.endpoint
      val endpointRefs = readerEndpointNames.map { endpointName =>
        rpcEnv.setupEndpointRef(rpcEnv.address, endpointName)
      }

      val runnables = prev.partitions.map { prevSplit =>
        new Runnable() {
          override def run(): Unit = {
            TaskContext.setTaskContext(context)

            val writer: ContinuousShuffleWriter = new RPCContinuousShuffleWriter(
              prevSplit.index, outputPartitioner, endpointRefs.toArray)

            EpochTracker.initializeCurrentEpoch(
              context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong)
            while (!context.isInterrupted() && !context.isCompleted()) {
              writer.write(prev.compute(prevSplit, context).asInstanceOf[Iterator[UnsafeRow]])
              // Note that current epoch is a non-inheritable thread local, so each writer thread
              // can properly increment its own epoch without affecting the main task thread.
              EpochTracker.incrementCurrentEpoch()
            }
          }
        }
      }

      context.addTaskCompletionListener[Unit] { ctx =>
        threadPool.shutdownNow()
      }

      part.writersInitialized = true

      runnables.foreach(threadPool.execute)
    }

    part.reader.read()
  }

  override def clearDependencies(): Unit = {
    throw new IllegalStateException("Continuous RDDs cannot be checkpointed")
  }
}
