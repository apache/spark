package spark.deploy

object ExecutorState extends Enumeration("LAUNCHING", "RUNNING", "FINISHED", "FAILED") {
  val LAUNCHING, RUNNING, FINISHED, FAILED = Value

  def isFinished(state: Value): Boolean = (state == FINISHED || state == FAILED)
}
