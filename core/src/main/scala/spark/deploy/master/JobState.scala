package spark.deploy.master

object JobState extends Enumeration("WAITING", "RUNNING", "FINISHED", "FAILED") {
  val WAITING, RUNNING, FINISHED, FAILED = Value
}
