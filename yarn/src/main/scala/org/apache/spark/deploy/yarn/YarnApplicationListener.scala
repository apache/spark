package org.apache.spark.deploy.yarn


import org.apache.hadoop.yarn.api.records.ApplicationId

sealed trait YarnApplicationEvent

case class YarnApplicationStart(time: Long) extends YarnApplicationEvent
case class YarnApplicationProgress(time: Long, progress: YarnAppProgress) extends YarnApplicationEvent
case class YarnApplicationEnd(time: Long) extends YarnApplicationEvent

trait YarnApplicationListener {
  def onApplicationInit(time:Long, appId: ApplicationId)
  def onApplicationStart(time:Long, info: YarnAppInfo)
  def onApplicationProgress(time:Long, progress: YarnAppProgress)
  def onApplicationEnd(time:Long, progress: YarnAppProgress)
  def onApplicationFailed(time:Long, progress: YarnAppProgress)
  def onApplicationKilled(time:Long, progress: YarnAppProgress)

}
