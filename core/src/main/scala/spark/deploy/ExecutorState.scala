package spark.deploy

private[spark] object ExecutorState
  extends Enumeration("LAUNCHING", "LOADING", "RUNNING", "KILLED", "FAILED", "LOST") {

  val LAUNCHING, LOADING, RUNNING, KILLED, FAILED, LOST = Value

  type ExecutorState = Value

  def isFinished(state: ExecutorState): Boolean = Seq(KILLED, FAILED, LOST).contains(state)
}
