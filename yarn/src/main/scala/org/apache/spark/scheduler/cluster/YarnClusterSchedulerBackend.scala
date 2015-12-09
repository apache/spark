/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.{ApplicationMaster, YarnSparkHadoopUtil}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc) {

  override def start() {
    val attemptId = ApplicationMaster.getAttemptId
    bindToYarn(attemptId.getApplicationId(), Some(attemptId))
    super.start()
    totalExpectedExecutors = YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sc.conf)
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    var driverLogs: Option[Map[String, String]] = None
    try {
      val yarnConf = new YarnConfiguration(sc.hadoopConfiguration)
      val containerId = YarnSparkHadoopUtil.get.getContainerId

      val httpAddress = System.getenv(Environment.NM_HOST.name()) +
        ":" + System.getenv(Environment.NM_HTTP_PORT.name())
      // lookup appropriate http scheme for container log urls
      val yarnHttpPolicy = yarnConf.get(
        YarnConfiguration.YARN_HTTP_POLICY_KEY,
        YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
      )
      val user = Utils.getCurrentUserName()
      val httpScheme = if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"
      val baseUrl = s"$httpScheme$httpAddress/node/containerlogs/$containerId/$user"
      logDebug(s"Base URL for logs: $baseUrl")
      driverLogs = Some(Map(
        "stderr" -> s"$baseUrl/stderr?start=-4096",
        "stdout" -> s"$baseUrl/stdout?start=-4096"))
    } catch {
      case e: Exception =>
        logInfo("Error while building AM log links, so AM" +
          " logs link will not appear in application UI", e)
    }
    driverLogs
  }
}
