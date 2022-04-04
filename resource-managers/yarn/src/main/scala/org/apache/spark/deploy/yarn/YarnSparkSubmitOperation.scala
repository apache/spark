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

package org.apache.spark.deploy.yarn

import scala.collection.Map

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{SparkHadoopUtil, SparkSubmitOperation}
import org.apache.spark.deploy.yarn.YarnSparkSubmitOperation._
import org.apache.spark.util.CommandLineLoggingUtils

class YarnSparkSubmitOperation
  extends SparkSubmitOperation with CommandLineLoggingUtils {

  private def withYarnClient(conf: SparkConf)(f: YarnClient => Unit): Unit = {
    val yarnClient = YarnClient.createYarnClient
    try {
      val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(conf))
      yarnClient.init(hadoopConf)
      yarnClient.start()
      f(yarnClient)
    } catch {
      case e: Exception =>
        printErrorAndExit(s"Failed to initialize yarn client due to ${e.getMessage}")
    } finally {
      yarnClient.stop()
    }
  }

  override def kill(applicationId: String, conf: SparkConf): Unit = {
    withYarnClient(conf) { yarnClient =>
      try {
        val appId = ApplicationId.fromString(applicationId)
        val report = yarnClient.getApplicationReport(appId)
        if (isTerminalState(report.getYarnApplicationState)) {
          printMessage(s"WARN: Application $appId is already terminated")
          printMessage(formatReportDetails(report))
        } else {
          yarnClient.killApplication(appId)
          val report = yarnClient.getApplicationReport(appId)
          printMessage(formatReportDetails(report))
        }

      } catch {
        case e: Exception =>
          printErrorAndExit(s"Failed to kill $applicationId due to ${e.getMessage}")
      }
    }
  }

  override def printSubmissionStatus(applicationId: String, conf: SparkConf): Unit = {
    withYarnClient(conf) { yarnClient =>
      try {
        val appId = ApplicationId.fromString(applicationId)
        val report = yarnClient.getApplicationReport(appId)
        report.getYarnApplicationState
        printMessage(formatReportDetails(report))
      } catch {
        case e: Exception =>
          printErrorAndExit(s"Failed to get app status due to ${e.getMessage}")
      }
    }
  }

  override def supports(master: String): Boolean = {
    master.startsWith("yarn")
  }
}

object YarnSparkSubmitOperation {
  def isTerminalState(state: YarnApplicationState): Boolean = {
    state match {
      case YarnApplicationState.FINISHED |
        YarnApplicationState.FAILED |
        YarnApplicationState.KILLED => true
      case _ => false
    }
  }

  /**
   * Return the security token used by this client to communicate with the ApplicationMaster.
   * If no security is enabled, the token returned by the report is null.
   */
  private def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToAMToken).map(_.toString).getOrElse("")

  /**
   * Format an application report and optionally, links to driver logs, in a human-friendly manner.
   *
   * @param report The application report from YARN.
   * @param driverLogsLinks A map of driver log files and their links. Keys are the file names
   *                        (e.g. `stdout`), and values are the links. If empty, nothing will be
   *                        printed.
   * @return Human-readable version of the input data.
   */
  def formatReportDetails(report: ApplicationReport,
      driverLogsLinks: Map[String, String] = Map.empty): String = {
    val details = Seq[(String, String)](
      ("client token", getClientToken(report)),
      ("diagnostics", report.getDiagnostics),
      ("ApplicationMaster host", report.getHost),
      ("ApplicationMaster RPC port", report.getRpcPort.toString),
      ("queue", report.getQueue),
      ("start time", report.getStartTime.toString),
      ("final status", report.getFinalApplicationStatus.toString),
      ("tracking URL", report.getTrackingUrl),
      ("user", report.getUser)
    ) ++ driverLogsLinks.map { case (fname, link) => (s"Driver Logs ($fname)", link) }

    // Use more loggable format if value is null or empty
    details.map { case (k, v) =>
      val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
      s"\n\t $k: $newValue"
    }.mkString("")
  }
}
