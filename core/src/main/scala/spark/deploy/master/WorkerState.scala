package spark.deploy.master

private[spark] object WorkerState extends Enumeration {
  type WorkerState = Value

  val ALIVE, DEAD, DECOMMISSIONED = Value
}
