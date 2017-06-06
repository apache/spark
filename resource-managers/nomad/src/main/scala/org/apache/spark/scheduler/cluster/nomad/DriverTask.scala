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

package org.apache.spark.scheduler.cluster.nomad

import java.net.URI
import java.util.Arrays.asList

import com.hashicorp.nomad.apimodel.{Service, Task}
import org.apache.http.HttpHost

import org.apache.spark.SparkConf
import org.apache.spark.deploy.nomad.ApplicationRunCommand
import org.apache.spark.deploy.nomad.NomadClusterModeLauncher.{PrimaryJar, PrimaryPythonFile, PrimaryRFile}
import org.apache.spark.internal.config.{DRIVER_MEMORY, PY_FILES}
import org.apache.spark.scheduler.cluster.nomad.SparkNomadJob.JOB_TEMPLATE
import org.apache.spark.util.Utils

private[spark] object DriverTask extends SparkNomadTaskType("driver", "driver", DRIVER_MEMORY) {

  private val PROPERTIES_NOT_TO_FORWARD = scala.collection.Set(
    "spark.master",
    "spark.driver.port",
    "spark.blockManager.port",
    "spark.ui.port",
    JOB_TEMPLATE.key,
    PY_FILES.key,
    "spark.app.id",
    "spark.app.name",
    "spark.submit.deployMode",
    "spark.driver.extraClassPath",
    "spark.driver.extraJavaOptions")

  case class Parameters(command: ApplicationRunCommand, nomadUrl: Option[HttpHost])

  def configure(
      jobConf: SparkNomadJob.CommonConf,
      conf: SparkConf,
      task: Task,
      parameters: Parameters
  ): Unit = {
    val sparkUIEnabled = conf.getBoolean("spark.ui.enabled", true)

    val driverPort = ConfigurablePort("driver")
    val blockManagerPort = ConfigurablePort("blockManager")
    val sparkUIPort =
      if (sparkUIEnabled) Some(ConfigurablePort("SparkUI"))
      else None
    val ports = Seq(driverPort, blockManagerPort) ++ sparkUIPort

    super.configure(jobConf, conf, task, ports, "spark-submit")

    val additionalJarUrls = Utils.getUserJars(conf).map(asFileIn(jobConf, task))
    if (additionalJarUrls.nonEmpty) {
      conf.set("spark.jars", additionalJarUrls.mkString(","))
    }

    conf.getOption("spark.files")
      .map(_.split(",").filter(_.nonEmpty))
      .filter(_.nonEmpty)
      .foreach { files =>
        conf.set("spark.files", files.map(asFileIn(jobConf, task)).mkString(","))
      }

    val driverConf: Seq[(String, String)] = {

      val explicitConf = Seq(
        "spark.app.id" -> jobConf.appId,
        "spark.app.name" -> jobConf.appName,
        "spark.driver.port" -> driverPort.placeholder,
        "spark.blockManager.port" -> blockManagerPort.placeholder
      ) ++ sparkUIPort.map("spark.ui.port" -> _.placeholder)

      val forwardedConf = conf.getAll
        .filterNot { case (name, _) => PROPERTIES_NOT_TO_FORWARD.contains(name) }

      explicitConf ++ forwardedConf
    }

    val command = parameters.command
    val submitOptions: Seq[String] = Seq(
      "--master=" + parameters.nomadUrl.fold("nomad")("nomad:" + _),
      "--driver-class-path=" +
        (additionalJarUrls ++ conf.getOption("spark.driver.extraClassPath"))
          .map(j => new URI(j).getPath).mkString(":")

    ) ++ driverConf.map { case (name, value) => s"--conf=$name=$value" }

    val primaryResourceUrl = asFileIn(jobConf, task)(command.primaryResource.url)
    val primaryResourceArgs: Seq[String] = command.primaryResource match {
      case PrimaryJar(_) =>
        Seq(s"--class=${command.mainClass}", primaryResourceUrl)
      case PrimaryPythonFile(_) =>
        val pythonOptions = conf.get(org.apache.spark.internal.config.PY_FILES) match {
          case Nil => Nil
          case pyFiles => Seq(pyFiles.map(asFileIn(jobConf, task))
            .mkString("--py-files=", ",", ""))
        }
        pythonOptions :+ primaryResourceUrl
      case PrimaryRFile(_) =>
        Seq(primaryResourceUrl)
    }

    appendArguments(task, submitOptions ++ primaryResourceArgs ++ command.arguments)

    sparkUIPort.foreach { port =>
      task.addServices(
        new Service()
          .setName(jobConf.appId
            .toLowerCase
            .replaceAll("[^a-z0-9]+", "-")
            .replaceAll("^-?([a-z0-9-]{62}[a-z0-9]?).*", "$1")
          )
          .setPortLabel(port.label)
          .setTags(asList("SparkUI"))
      )
    }

  }

}
