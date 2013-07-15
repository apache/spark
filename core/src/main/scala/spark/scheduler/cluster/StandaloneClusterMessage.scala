package spark.scheduler.cluster

import spark.TaskState.TaskState
import java.nio.ByteBuffer
import spark.util.SerializableBuffer
import spark.Utils

private[spark] sealed trait StandaloneClusterMessage extends Serializable

// Driver to executors
private[spark]
case class LaunchTask(task: TaskDescription) extends StandaloneClusterMessage

private[spark]
case class RegisteredExecutor(sparkProperties: Seq[(String, String)])
  extends StandaloneClusterMessage

private[spark]
case class RegisterExecutorFailed(message: String) extends StandaloneClusterMessage

// Executors to driver
private[spark]
case class RegisterExecutor(executorId: String, hostPort: String, cores: Int)
  extends StandaloneClusterMessage {
  Utils.checkHostPort(hostPort, "Expected host port")
}

private[spark]
case class StatusUpdate(executorId: String, taskId: Long, state: TaskState, data: SerializableBuffer)
  extends StandaloneClusterMessage

private[spark]
object StatusUpdate {
  /** Alternate factory method that takes a ByteBuffer directly for the data field */
  def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer): StatusUpdate = {
    StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
  }
}

// Internal messages in driver
private[spark] case object ReviveOffers extends StandaloneClusterMessage
private[spark] case object StopDriver extends StandaloneClusterMessage

private[spark] case class RemoveExecutor(executorId: String, reason: String)
  extends StandaloneClusterMessage
