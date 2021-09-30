
package org.apache.spark

private[spark] object TaskState extends Enumeration {

  val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value

  private val FINISHED_STATES = Set(FINISHED, FAILED, KILLED, LOST)

  type TaskState = Value

  def isFailed(state: TaskState): Boolean = (LOST == state) || (FAILED == state)

  def isFinished(state: TaskState): Boolean = FINISHED_STATES.contains(state)
}
