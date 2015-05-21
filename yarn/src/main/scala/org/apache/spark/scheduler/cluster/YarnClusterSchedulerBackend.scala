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

import java.net.NetworkInterface

import scala.collection.JavaConverters._

import org.apache.hadoop.yarn.api.records.NodeState
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.{IntParam, Utils}

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc) {

  override def start() {
    super.start()
    totalExpectedExecutors = DEFAULT_NUMBER_EXECUTORS
    if (System.getenv("SPARK_EXECUTOR_INSTANCES") != null) {
      totalExpectedExecutors = IntParam.unapply(System.getenv("SPARK_EXECUTOR_INSTANCES"))
        .getOrElse(totalExpectedExecutors)
    }
    // System property can override environment variable.
    totalExpectedExecutors = sc.getConf.getInt("spark.executor.instances", totalExpectedExecutors)
  }

  override def applicationId(): String =
    // In YARN Cluster mode, the application ID is expected to be set, so log an error if it's
    // not found.
    sc.getConf.getOption("spark.yarn.app.id").getOrElse {
      logError("Application ID is not set.")
      super.applicationId
    }

  override def applicationAttemptId(): Option[String] =
    // In YARN Cluster mode, the attempt ID is expected to be set, so log an error if it's
    // not found.
    sc.getConf.getOption("spark.yarn.app.attemptId").orElse {
      logError("Application attempt ID is not set.")
      super.applicationAttemptId
    }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    var yarnClientOpt: Option[YarnClient] = None
    var driverLogs: Option[Map[String, String]] = None
    try {
      val yarnConf = new YarnConfiguration(sc.hadoopConfiguration)
      val containerId = YarnSparkHadoopUtil.get.getContainerId
      yarnClientOpt = Some(YarnClient.createYarnClient())
      yarnClientOpt.foreach { yarnClient =>
        yarnClient.init(yarnConf)
        yarnClient.start()

        // For newer versions of YARN, we can find the HTTP address for a given node by getting a
        // container report for a given container. But container reports came only in Hadoop 2.4,
        // so we basically have to get the node reports for all nodes and find the one which runs
        // this container. For that we have to compare the node's host against the current host.
        // Since the host can have multiple addresses, we need to compare against all of them to
        // find out if one matches.

        // Get all the addresses of this node.
        val addresses =
          NetworkInterface.getNetworkInterfaces.asScala
            .flatMap(_.getInetAddresses.asScala)
            .toSeq

        // Find a node report that matches one of the addresses
        val nodeReport =
          yarnClient.getNodeReports(NodeState.RUNNING).asScala.find { x =>
            val host = x.getNodeId.getHost
            addresses.exists { address =>
              address.getHostAddress == host ||
                address.getHostName == host ||
                address.getCanonicalHostName == host
            }
          }

        // Now that we have found the report for the Node Manager that the AM is running on, we
        // can get the base HTTP address for the Node manager from the report.
        // The format used for the logs for each container is well-known and can be constructed
        // using the NM's HTTP address and the container ID.
        // The NM may be running several containers, but we can build the URL for the AM using
        // the AM's container ID, which we already know.
        nodeReport.foreach { report =>
          val httpAddress = report.getHttpAddress
          // lookup appropriate http scheme for container log urls
          val yarnHttpPolicy = yarnConf.get(
            YarnConfiguration.YARN_HTTP_POLICY_KEY,
            YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
          )
          val user = Utils.getCurrentUserName()
          val httpScheme = if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"
          val baseUrl = s"$httpScheme$httpAddress/node/containerlogs/$containerId/$user"
          logDebug(s"Base URL for logs: $baseUrl")
          driverLogs = Some(
            Map("stderr" -> s"$baseUrl/stderr?start=0", "stdout" -> s"$baseUrl/stdout?start=0"))
        }
      }
    } catch {
      case e: Exception =>
        logInfo("Node Report API is not available in the version of YARN being used, so AM" +
          " logs link will not appear in application UI", e)
    } finally {
      yarnClientOpt.foreach(_.close())
    }
    driverLogs
  }
}
