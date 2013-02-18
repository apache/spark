package spark.deploy.master

private[spark] object ApplicationState
  extends Enumeration("WAITING", "RUNNING", "FINISHED", "FAILED") {

  type ApplicationState = Value

  val WAITING, RUNNING, FINISHED, FAILED = Value

  val MAX_NUM_RETRY = 10
}
