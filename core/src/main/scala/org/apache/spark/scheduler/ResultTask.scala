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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import java.io._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param rddBinary broadcast version of of the serialized RDD
 * @param func a function to apply on a partition of the RDD
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    val rddBinary: Broadcast[Array[Byte]],
    val func: (TaskContext, Iterator[T]) => U,
    val partition: Partition,
    @transient locs: Seq[TaskLocation],
    val outputId: Int)
  extends Task[U](stageId, partition.index) with Serializable {

  // TODO: Should we also broadcast func? For that we would need a place to
  // keep a reference to it (perhaps in DAGScheduler's job object).

  def this(
      stageId: Int,
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitionId: Int,
      locs: Seq[TaskLocation],
      outputId: Int) = {
    this(stageId, rdd.broadcasted, func, rdd.partitions(partitionId), locs, outputId)
  }

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val rdd = ser.deserialize[RDD[T]](ByteBuffer.wrap(rddBinary.value),
      Thread.currentThread.getContextClassLoader)
    metrics = Some(context.taskMetrics)
    try {
      func(context, rdd.iterator(partition, context))
    } finally {
      context.executeOnCompleteCallbacks()
    }
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ResultTask(" + stageId + ", " + partitionId + ")"
}
