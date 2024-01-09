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

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{ExecutorLossReason, MiscellaneousProcessDetails}
import org.apache.spark.util.SerializableBuffer

private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessages {

  case class RetrieveSparkAppConfig(resourceProfileId: Int) extends CoarseGrainedClusterMessage

  case class SparkAppConfig(
      sparkProperties: Seq[(String, String)],
      ioEncryptionKey: Option[Array[Byte]],
      hadoopDelegationCreds: Option[Array[Byte]],
      resourceProfile: ResourceProfile,
      logLevel: Option[String])
    extends CoarseGrainedClusterMessage

  case object RetrieveLastAllocatedExecutorId extends CoarseGrainedClusterMessage

  // Driver to executors
  case class LaunchTask(data: SerializableBuffer) extends CoarseGrainedClusterMessage

  case class KillTask(taskId: Long, executor: String, interruptThread: Boolean, reason: String)
    extends CoarseGrainedClusterMessage

  case class KillExecutorsOnHost(host: String)
    extends CoarseGrainedClusterMessage

  case class UpdateExecutorsLogLevel(logLevel: String) extends CoarseGrainedClusterMessage

  case class UpdateExecutorLogLevel(logLevel: String) extends CoarseGrainedClusterMessage

  case class DecommissionExecutorsOnHost(host: String)
    extends CoarseGrainedClusterMessage

  case class UpdateDelegationTokens(tokens: Array[Byte])
    extends CoarseGrainedClusterMessage

  // Executors to driver
  case class RegisterExecutor(
      executorId: String,
      executorRef: RpcEndpointRef,
      hostname: String,
      cores: Int,
      logUrls: Map[String, String],
      attributes: Map[String, String],
      resources: Map[String, ResourceInformation],
      resourceProfileId: Int)
    extends CoarseGrainedClusterMessage

  case class LaunchedExecutor(executorId: String) extends CoarseGrainedClusterMessage

  case class StatusUpdate(
      executorId: String,
      taskId: Long,
      state: TaskState,
      data: SerializableBuffer,
      taskCpus: Int,
      resources: Map[String, Map[String, Long]] = Map.empty)
    extends CoarseGrainedClusterMessage

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(
        executorId: String,
        taskId: Long,
        state: TaskState,
        data: ByteBuffer,
        taskCpus: Int,
        resources: Map[String, Map[String, Long]]): StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data), taskCpus, resources)
    }
  }

  case class ShufflePushCompletion(shuffleId: Int, shuffleMergeId: Int, mapIndex: Int)
    extends CoarseGrainedClusterMessage

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessage

  case object StopDriver extends CoarseGrainedClusterMessage

  case object StopExecutor extends CoarseGrainedClusterMessage

  case object StopExecutors extends CoarseGrainedClusterMessage

  case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
    extends CoarseGrainedClusterMessage

  // A message that sent from executor to driver to tell driver that the executor has started
  // decommissioning. It's used for the case where decommission is triggered at executor (e.g., K8S)
  case class ExecutorDecommissioning(executorId: String) extends CoarseGrainedClusterMessage

  // A message that sent from driver to executor to decommission that executor.
  // It's used for Standalone's cases, where decommission is triggered at MasterWebUI or Worker.
  object DecommissionExecutor extends CoarseGrainedClusterMessage

  // A message that sent to the executor itself when it receives a signal,
  // indicating the executor starts to decommission.
  object ExecutorDecommissionSigReceived extends CoarseGrainedClusterMessage

  case class RemoveWorker(workerId: String, host: String, message: String)
    extends CoarseGrainedClusterMessage

  case class SetupDriver(driver: RpcEndpointRef) extends CoarseGrainedClusterMessage

  // Exchanged between the driver and the AM in Yarn client mode
  case class AddWebUIFilter(
      filterName: String, filterParams: Map[String, String], proxyBase: String)
    extends CoarseGrainedClusterMessage

  // Messages exchanged between the driver and the cluster manager for executor allocation
  // In Yarn mode, these are exchanged between the driver and the AM

  case class RegisterClusterManager(am: RpcEndpointRef) extends CoarseGrainedClusterMessage

  // Send Miscellaneous Process information to the driver
  case class MiscellaneousProcessAdded(
      time: Long, processId: String, info: MiscellaneousProcessDetails)
    extends CoarseGrainedClusterMessage

  // Used by YARN's client mode AM to retrieve the current set of delegation tokens.
  object RetrieveDelegationTokens extends CoarseGrainedClusterMessage

  // Request executors by specifying the new total number of executors desired
  // This includes executors already pending or running
  case class RequestExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]],
      excludedNodes: Set[String])
    extends CoarseGrainedClusterMessage

  // Check if an executor was force-killed but for a reason unrelated to the running tasks.
  // This could be the case if the executor is preempted, for instance.
  case class GetExecutorLossReason(executorId: String) extends CoarseGrainedClusterMessage

  case class KillExecutors(executorIds: Seq[String]) extends CoarseGrainedClusterMessage

  // Used internally by executors to shut themselves down.
  case class Shutdown(exitCode: Int = 0) extends CoarseGrainedClusterMessage

  // The message to check if `CoarseGrainedSchedulerBackend` thinks the executor is alive or not.
  case class IsExecutorAlive(executorId: String) extends CoarseGrainedClusterMessage

  case class TaskThreadDump(taskId: Long) extends CoarseGrainedClusterMessage
}
