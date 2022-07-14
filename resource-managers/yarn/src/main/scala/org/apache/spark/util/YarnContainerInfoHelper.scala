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

  private[this] val DRIVER_LOG_FILE_NAMES = Seq("stdout", "stderr")
  private[this] val DRIVER_LOG_START_OFFSET = -4096

  def getLogUrlsFromBaseUrl(baseUrl: String): Map[String, String] = {
    DRIVER_LOG_FILE_NAMES.map { fname =>
      fname -> s"$baseUrl/$fname?start=$DRIVER_LOG_START_OFFSET"
    }.toMap
  }

  def getLogUrls(
      conf: Configuration,
      container: Option[Container]): Option[Map[String, String]] = {
    try {
      val yarnConf = new YarnConfiguration(conf)

      val containerId = getContainerId(container)
      val user = Utils.getCurrentUserName()
      val httpScheme = getYarnHttpScheme(yarnConf)
      val httpAddress = getNodeManagerHttpAddress(container)

      val baseUrl = s"$httpScheme$httpAddress/node/containerlogs/$containerId/$user"
      logDebug(s"Base URL for logs: $baseUrl")

      Some(getLogUrlsFromBaseUrl(baseUrl))
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
      Some(Map(
        "HTTP_SCHEME" -> getYarnHttpScheme(yarnConf),
        "NM_HOST" -> getNodeManagerHost(container),
        "NM_PORT" -> getNodeManagerPort(container),
        "NM_HTTP_PORT" -> getNodeManagerHttpPort(container),
        "NM_HTTP_ADDRESS" -> getNodeManagerHttpAddress(container),
        "CLUSTER_ID" -> getClusterId(yarnConf).getOrElse(""),
        "CONTAINER_ID" -> ConverterUtils.toString(getContainerId(container)),
        "USER" -> Utils.getCurrentUserName(),
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

  def getNodeManagerHttpAddress(container: Option[Container]): String = container match {
    case Some(c) => c.getNodeHttpAddress
    case None => getNodeManagerHost(None) + ":" + getNodeManagerHttpPort(None)
  }

  def getNodeManagerHost(container: Option[Container]): String = container match {
    case Some(c) => c.getNodeHttpAddress.split(":")(0)
    case None => System.getenv(Environment.NM_HOST.name())
  }

  def getNodeManagerHttpPort(container: Option[Container]): String = container match {
    case Some(c) => c.getNodeHttpAddress.split(":")(1)
    case None => System.getenv(Environment.NM_HTTP_PORT.name())
  }

  def getNodeManagerPort(container: Option[Container]): String = container match {
    case Some(_) => "-1" // Just return invalid port given we cannot retrieve the value
    case None => System.getenv(Environment.NM_PORT.name())
  }

}
