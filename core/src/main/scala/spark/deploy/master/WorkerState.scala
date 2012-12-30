package spark.deploy.master

private[spark] object WorkerState extends Enumeration("ALIVE", "DEAD", "DECOMMISSIONED") {
  type WorkerState = Value

  val ALIVE, DEAD, DECOMMISSIONED = Value
}
