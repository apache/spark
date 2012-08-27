package spark.deploy.master

object JobState extends Enumeration("WAITING", "RUNNING", "FINISHED", "FAILED") {
  type JobState = Value

  val WAITING, RUNNING, FINISHED, FAILED = Value
}
