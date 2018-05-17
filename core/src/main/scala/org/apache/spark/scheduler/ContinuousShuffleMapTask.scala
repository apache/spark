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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 * @param totalShuffleNum total shuffle number for current job.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] class ContinuousShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    totalShuffleNum: Int,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[Unit](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId)
    with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int, totalShuffleNum: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties,
      null, totalShuffleNum)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  // TODO: Get current epoch from epoch coordinator while task restart, also epoch is Long, we
  //       should deal with it.
  var currentEpoch = localProperties.getProperty(SparkEnv.START_EPOCH_KEY).toInt

  override def runTask(context: TaskContext): Unit = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    // TODO: rdd here should be a wrap of ShuffledRowRDD which never stop
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    val manager = SparkEnv.get.shuffleManager
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
      .asInstanceOf[ContinuousMapOutputTrackerWorker]

    while (!context.isCompleted() || !context.isInterrupted()) {
      try {
        // Create a ContinuousShuffleDependency which has new shuffleId based on continuous epoch.
        // Since rdd in the dependency will not be used, set null to avoid compile issues.
        val continuousDep = new ContinuousShuffleDependency(
          null, dep, currentEpoch, totalShuffleNum,
          rdd.partitions.length)
        // Re-register the shuffle TO mapOutputTrackerMaster
        mapOutputTracker.checkAndRegisterShuffle(continuousDep.shuffleId, rdd.partitions.length)
        writer = manager.getWriter[Any, Any](continuousDep.shuffleHandle, partitionId, context)
        writer.write(rdd.iterator(partition, context)
          .asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
        val status = writer.stop(success = true).get
        // Register map output in task cause the continuous task never success
        mapOutputTracker.registerMapOutput(continuousDep.shuffleId, partitionId, status)
        currentEpoch += 1
      } catch {
        case e: Exception =>
          try {
            if (writer != null) {
              writer.stop(success = false)
            }
          } catch {
            case e: Exception =>
              log.debug("Could not stop writer", e)
          }
          throw e
      }
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ContinuousShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
