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

package org.apache.spark.util

import org.apache.hadoop.HadoopIllegalArgumentException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{Container, ContainerId}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil
import org.apache.spark.internal.Logging

private[spark] object YarnContainerInfoHelper extends Logging {
  def getLogUrls(
      conf: Configuration,
      container: Option[Container]): Option[Map[String, String]] = {
    try {
      val yarnConf = new YarnConfiguration(conf)

      val containerId = getContainerId(container)
      val user = Utils.getCurrentUserName()
      val httpScheme = getYarnHttpScheme(yarnConf)
      val host = getNodeManagerHost
      val httpPort = getNodeManagerHttpPort

      val baseUrl = s"$httpScheme$host:$httpPort/node/containerlogs/$containerId/$user"
      logDebug(s"Base URL for logs: $baseUrl")

      Some(Map(
        "stdout" -> s"$baseUrl/stdout?start=-4096",
        "stderr" -> s"$baseUrl/stderr?start=-4096"))
    } catch {
      case e: Exception =>
        logInfo("Error while building executor logs - executor logs will not be available", e)
        None
    }
  }

  def getAttributes(
      conf: Configuration,
      container: Option[Container]): Option[Map[String, String]] = {
    try {
      val yarnConf = new YarnConfiguration(conf)

      val clusterId = getClusterId(yarnConf)
      val containerId = getContainerId(container)
      val host = getNodeManagerHost
      val port = getNodeManagerPort
      val httpPort = getNodeManagerHttpPort
      val user = Utils.getCurrentUserName()
      val httpScheme = getYarnHttpScheme(yarnConf)

      Some(Map(
        "HTTP_SCHEME" -> httpScheme,
        "NODE_HOST" -> host,
        "NODE_PORT" -> port.toString,
        "NODE_HTTP_PORT" -> httpPort.toString,
        "CLUSTER_ID" -> clusterId.getOrElse(""),
        "CONTAINER_ID" -> ConverterUtils.toString(containerId),
        "USER" -> user,
        "LOG_FILES" -> "stderr,stdout"
      ))
    } catch {
      case e: Exception =>
        logInfo("Error while retrieving executor attributes - executor logs will not be replaced " +
          "with custom log pattern", e)
        None
    }
  }

  def getContainerId(container: Option[Container]): ContainerId = container match {
    case Some(c) => c.getId
    case None => YarnSparkHadoopUtil.getContainerId
  }

  def getClusterId(yarnConf: YarnConfiguration): Option[String] = {
    try {
      Some(YarnConfiguration.getClusterId(yarnConf))
    } catch {
      case _: HadoopIllegalArgumentException => None
    }
  }

  def getYarnHttpScheme(yarnConf: YarnConfiguration): String = {
    // lookup appropriate http scheme for container log urls
    val yarnHttpPolicy = yarnConf.get(
      YarnConfiguration.YARN_HTTP_POLICY_KEY,
      YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
    )
    if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"
  }

  def getNodeManagerHost: String = System.getenv(Environment.NM_HOST.name())

  def getNodeManagerPort: Int = System.getenv(Environment.NM_PORT.name()).toInt

  def getNodeManagerHttpPort: Int = System.getenv(Environment.NM_HTTP_PORT.name()).toInt
}
