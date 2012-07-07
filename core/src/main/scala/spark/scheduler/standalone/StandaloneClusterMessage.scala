package spark.scheduler.standalone

import spark.TaskState.TaskState
import spark.scheduler.cluster.TaskDescription

sealed trait StandaloneClusterMessage extends Serializable

case class RegisterSlave(slaveId: String, host: String, cores: Int) extends StandaloneClusterMessage
case class LaunchTask(slaveId: String, task: TaskDescription) extends StandaloneClusterMessage

case class StatusUpdate(slaveId: String, taskId: Long, state: TaskState, data: Array[Byte])
  extends StandaloneClusterMessage

case object ReviveOffers extends StandaloneClusterMessage
case object StopMaster extends StandaloneClusterMessage

