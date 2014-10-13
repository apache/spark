package org.apache.spark.autoscale

private[spark] sealed trait AutoscaleMessage extends Serializable

private[spark] object AutoscaleMessages {
  case class AddExecutors(count: Int) extends AutoscaleMessage
  case class DeleteExecutors(execId: List[String]) extends AutoscaleMessage
  case object DeleteFreeExecutors extends AutoscaleMessage
  case object RegisterAutoscaleServer extends AutoscaleMessage
  case object RegisteredAutoscaleServer extends AutoscaleMessage
}