package org.apache.spark.deploy.yarn


case class JobArg(name: String, value: Any)

sealed trait SparkJobCommand

case class StartSparkJob(name: String, args: JobArg*) extends SparkJobCommand
case class StopSparkJob(name: String, args: JobArg*) extends SparkJobCommand

sealed trait SparkJobStatus {
  val name: String
  val message: String
}
case class SubmittingSparkJob(name: String, message: String) extends SparkJobStatus
case class SparkJobStarted(name: String, message: String) extends SparkJobStatus
case class SparkJobFinished(name: String, message: String) extends SparkJobStatus
case class SparkJobFailed(name: String, message: String, cause: Throwable) extends SparkJobStatus
case class SparkJobProgress(name: String, message: String) extends SparkJobStatus


