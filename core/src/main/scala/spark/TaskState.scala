package spark

import org.apache.mesos.Protos.{TaskState => MesosTaskState}

private[spark] object TaskState
  extends Enumeration("LAUNCHING", "RUNNING", "FINISHED", "FAILED", "KILLED", "LOST") {

  val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value

  type TaskState = Value

  def isFinished(state: TaskState) = Seq(FINISHED, FAILED, LOST).contains(state)

  def toMesos(state: TaskState): MesosTaskState = state match {
    case LAUNCHING => MesosTaskState.TASK_STARTING
    case RUNNING => MesosTaskState.TASK_RUNNING
    case FINISHED => MesosTaskState.TASK_FINISHED
    case FAILED => MesosTaskState.TASK_FAILED
    case KILLED => MesosTaskState.TASK_KILLED
    case LOST => MesosTaskState.TASK_LOST
  }

  def fromMesos(mesosState: MesosTaskState): TaskState = mesosState match {
    case MesosTaskState.TASK_STAGING => LAUNCHING
    case MesosTaskState.TASK_STARTING => LAUNCHING
    case MesosTaskState.TASK_RUNNING => RUNNING
    case MesosTaskState.TASK_FINISHED => FINISHED
    case MesosTaskState.TASK_FAILED => FAILED
    case MesosTaskState.TASK_KILLED => KILLED
    case MesosTaskState.TASK_LOST => LOST
  }
}
