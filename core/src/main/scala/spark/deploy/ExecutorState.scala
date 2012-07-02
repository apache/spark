package spark.deploy

object ExecutorState
  extends Enumeration("LAUNCHING", "LOADING", "RUNNING", "KILLED", "FAILED", "LOST") {

  val LAUNCHING, LOADING, RUNNING, KILLED, FAILED, LOST = Value

  def isFinished(state: Value): Boolean = (state == KILLED || state == FAILED || state == LOST)
}
