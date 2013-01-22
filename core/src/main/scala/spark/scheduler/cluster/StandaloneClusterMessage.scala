package spark.scheduler.cluster

import spark.TaskState.TaskState
import java.nio.ByteBuffer
import spark.util.SerializableBuffer

private[spark] sealed trait StandaloneClusterMessage extends Serializable

// Driver to executors
private[spark]
case class LaunchTask(task: TaskDescription) extends StandaloneClusterMessage

private[spark]
case class RegisteredSlave(sparkProperties: Seq[(String, String)]) extends StandaloneClusterMessage

private[spark]
case class RegisterSlaveFailed(message: String) extends StandaloneClusterMessage

// Executors to driver
private[spark]
case class RegisterSlave(slaveId: String, host: String, cores: Int) extends StandaloneClusterMessage

private[spark]
case class StatusUpdate(slaveId: String, taskId: Long, state: TaskState, data: SerializableBuffer)
  extends StandaloneClusterMessage

private[spark]
object StatusUpdate {
  /** Alternate factory method that takes a ByteBuffer directly for the data field */
  def apply(slaveId: String, taskId: Long, state: TaskState, data: ByteBuffer): StatusUpdate = {
    StatusUpdate(slaveId, taskId, state, new SerializableBuffer(data))
  }
}

// Internal messages in driver
private[spark] case object ReviveOffers extends StandaloneClusterMessage
private[spark] case object StopDriver extends StandaloneClusterMessage
