package spark.scheduler.cluster

import spark.TaskState.TaskState
import java.nio.ByteBuffer
import spark.util.SerializableBuffer

sealed trait StandaloneClusterMessage extends Serializable

// Master to slaves
case class LaunchTask(slaveId: String, task: TaskDescription) extends StandaloneClusterMessage
case class RegisteredSlave(sparkProperties: Seq[(String, String)]) extends StandaloneClusterMessage
case class RegisterSlaveFailed(message: String) extends StandaloneClusterMessage

// Slaves to master
case class RegisterSlave(slaveId: String, host: String, cores: Int) extends StandaloneClusterMessage

case class StatusUpdate(slaveId: String, taskId: Long, state: TaskState, data: SerializableBuffer)
  extends StandaloneClusterMessage

object StatusUpdate {
  /** Alternate factory method that takes a ByteBuffer directly for the data field */
  def apply(slaveId: String, taskId: Long, state: TaskState, data: ByteBuffer): StatusUpdate = {
    StatusUpdate(slaveId, taskId, state, new SerializableBuffer(data))
  }
}

// Internal messages in master
case object ReviveOffers extends StandaloneClusterMessage
case object StopMaster extends StandaloneClusterMessage
