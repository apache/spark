package org.apache.spark.deploy.yarn.server

import org.apache.hadoop.yarn.api.records.ApplicationId
// this requires SPARK-3913, comment this for now
//import org.apache.spark.deploy.yarn.{YarnAppProgress, YarnAppInfo}


sealed trait YarnAppState
case class AppInit(time: Long, appId: ApplicationId) extends YarnAppState
/*
 this requires SPARK-3913, comment this for now

case class AppStart(time: Long, appInfo: YarnAppInfo) extends YarnAppState
case class AppProgress(time: Long, progress: YarnAppProgress) extends YarnAppState
case class AppKilled(time: Long, progress: YarnAppProgress) extends YarnAppState
case class AppEnd(time: Long, progress: YarnAppProgress) extends YarnAppState
case class AppFailed(time: Long, progress: YarnAppProgress) extends YarnAppState
*/

case class AppStart(time: Long) extends YarnAppState
case class AppProgress(time: Long) extends YarnAppState
case class AppKilled(time: Long) extends YarnAppState
case class AppEnd(time: Long) extends YarnAppState
case class AppFailed(time: Long) extends YarnAppState
