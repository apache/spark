
package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.ExecutorLossReason
import org.apache.spark.util.SerializableBuffer

private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessages {

  case object RetrieveSparkAppConfig extends CoarseGrainedClusterMessage

  case class SparkAppConfig(
      sparkProperties: Seq[(String, String)],
      ioEncryptionKey: Option[Array[Byte]],
      hadoopDelegationCreds: Option[Array[Byte]])
    extends CoarseGrainedClusterMessage

  case object RetrieveLastAllocatedExecutorId extends CoarseGrainedClusterMessage

  // Driver to executors
  case class LaunchTask(data: SerializableBuffer) extends CoarseGrainedClusterMessage

  case class KillTask(taskId: Long, executor: String, interruptThread: Boolean, reason: String)
    extends CoarseGrainedClusterMessage

  case class KillExecutorsOnHost(host: String)
    extends CoarseGrainedClusterMessage

  sealed trait RegisterExecutorResponse

  case object RegisteredExecutor extends CoarseGrainedClusterMessage with RegisterExecutorResponse

  case class RegisterExecutorFailed(message: String) extends CoarseGrainedClusterMessage
    with RegisterExecutorResponse

  case class UpdateDelegationTokens(tokens: Array[Byte])
    extends CoarseGrainedClusterMessage

  // Executors to driver
  case class RegisterExecutor(
      executorId: String,
      executorRef: RpcEndpointRef,
      hostname: String,
      cores: Int,
      logUrls: Map[String, String])
    extends CoarseGrainedClusterMessage

  case class StatusUpdate(executorId: String, taskId: Long, state: TaskState,
    data: SerializableBuffer) extends CoarseGrainedClusterMessage

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer)
      : StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
    }
  }

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessage

  case object StopDriver extends CoarseGrainedClusterMessage

  case object StopExecutor extends CoarseGrainedClusterMessage

  case object StopExecutors extends CoarseGrainedClusterMessage

  case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
    extends CoarseGrainedClusterMessage

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

  // Request executors by specifying the new total number of executors desired
  // This includes executors already pending or running
  case class RequestExecutors(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int],
      nodeBlacklist: Set[String])
    extends CoarseGrainedClusterMessage

  // Check if an executor was force-killed but for a reason unrelated to the running tasks.
  // This could be the case if the executor is preempted, for instance.
  case class GetExecutorLossReason(executorId: String) extends CoarseGrainedClusterMessage

  case class KillExecutors(executorIds: Seq[String]) extends CoarseGrainedClusterMessage

  // Used internally by executors to shut themselves down.
  case object Shutdown extends CoarseGrainedClusterMessage

}
