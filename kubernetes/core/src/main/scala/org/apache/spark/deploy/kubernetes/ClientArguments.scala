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
package org.apache.spark.deploy.kubernetes

import org.apache.commons.lang3.Validate
import scala.collection.mutable

import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.SparkConf

private[spark] case class ClientArguments(
    kubernetesAppName: Option[String],
    userMainClass: String,
    userArgs: Array[String],
    jars: Array[String],
    additionalDriverPorts: Map[String, Int],
    sparkConf: Map[String, String],
    driverExtraClassPath: Array[String],
    driverDockerImage: String,
    executorDockerImage: String,
    customExecutorSpecFile: Option[String],
    customExecutorSpecContainerName: Option[String],
    kubernetesMaster: String,
    kubernetesAppNamespace: String,
    kubernetesClientCertFile: Option[String] = Option.empty,
    kubernetesClientKeyFile: Option[String] = Option.empty,
    kubernetesCaCertFile: Option[String] = Option.empty)

private[spark] object ClientArguments {
  def builder(): ClientArgumentsBuilder = new ClientArgumentsBuilder
}

private[spark] case class ClientArgumentsBuilder(
    kubernetesAppName: Option[String] = None,
    userMainClass: Option[String] = Option.empty[String],
    userArgs: Seq[String] = Seq.empty[String],
    jars: Seq[String] = Seq.empty[String],
    driverExtraClassPath: Seq[String] = Seq.empty[String],
    additionalDriverPorts: Map[String, Int] = Map[String, Int](),
    sparkConf: Map[String, String] = Map[String, String](),
    driverDockerImage: Option[String] = None, // TODO use the precise version
    executorDockerImage: Option[String] = None,
    customExecutorSpecFile: Option[String] = None,
    customExecutorSpecContainerName: Option[String] = None,
    kubernetesMaster: Option[String] = None,
    kubernetesAppNamespace: Option[String] = None,
    kubernetesClientCertFile: Option[String] = None,
    kubernetesClientKeyFile: Option[String] = None,
    kubernetesCaCertFile: Option[String] = None) extends SparkSubmitArgumentsParser {

  def kubernetesAppName(kubernetesAppName: String): ClientArgumentsBuilder = {
    this.copy(kubernetesAppName = Some(Validate.notBlank(kubernetesAppName)))
  }

  def userMainClass(userMainClass: String): ClientArgumentsBuilder = {
    this.copy(userMainClass = Some(Validate.notBlank(userMainClass)))
  }

  def addUserArg(userArg: String): ClientArgumentsBuilder = {
    this.copy(userArgs = this.userArgs :+ Validate.notBlank(userArg))
  }

  def addJar(jar: String): ClientArgumentsBuilder = {
    this.copy(jars = this.jars :+ Validate.notBlank(jar))
  }

  def addDriverExtraClassPath(driverExtraClassPathEntry: String): ClientArgumentsBuilder = {
    this.copy(driverExtraClassPath = this.driverExtraClassPath
      :+ Validate.notBlank(driverExtraClassPathEntry))
  }

  def addSparkConf(conf: String, value: String): ClientArgumentsBuilder = {
    this.copy(sparkConf = this.sparkConf + (Validate.notBlank(conf) -> Validate.notBlank(value)))
  }

  def addDriverPort(portName: String, value: Int): ClientArgumentsBuilder = {
    Validate.isTrue(value > 0, s"Port with name $portName must be positive, got: $value")
    this.copy(additionalDriverPorts = this.additionalDriverPorts
      + (Validate.notBlank(portName) -> value))
  }

  def driverDockerImage(driverDockerImage: String): ClientArgumentsBuilder = {
    this.copy(driverDockerImage = Some(Validate.notBlank(driverDockerImage)))
  }

  def executorDockerImage(executorDockerImage: String): ClientArgumentsBuilder = {
    this.copy(executorDockerImage = Some(Validate.notBlank(executorDockerImage)))
  }

  def customExecutorSpecFile(customExecutorSpecFile: String): ClientArgumentsBuilder = {
    this.copy(customExecutorSpecFile = Some(Validate.notBlank(customExecutorSpecFile)))
  }

  def customExecutorSpecContainerName(customExecutorSpecContainerName: String)
      : ClientArgumentsBuilder = {
    this.copy(customExecutorSpecContainerName =
      Some(Validate.notBlank(customExecutorSpecContainerName)))
  }

  def kubernetesMaster(kubernetesMaster: String): ClientArgumentsBuilder = {
    this.copy(kubernetesMaster = Some(Validate.notBlank(kubernetesMaster)))
  }

  def kubernetesAppNamespace(kubernetesAppNamespace: String): ClientArgumentsBuilder = {
    this.copy(kubernetesAppNamespace = Some(Validate.notBlank(kubernetesAppNamespace)))
  }

  def kubernetesClientCertFile(kubernetesClientCertFile: String): ClientArgumentsBuilder = {
    this.copy(kubernetesClientCertFile = Some(Validate.notBlank(kubernetesClientCertFile)))
  }

  def kubernetesClientKeyFile(kubernetesClientKeyFile: String): ClientArgumentsBuilder = {
    this.copy(kubernetesClientKeyFile = Some(Validate.notBlank(kubernetesClientKeyFile)))
  }

  def kubernetesCaCertFile(kubernetesCaCertFile: String): ClientArgumentsBuilder = {
    this.copy(kubernetesCaCertFile = Some(Validate.notBlank(kubernetesCaCertFile)))
  }

  def fromArgsArray(inputArgs: Array[String]): ClientArgumentsBuilder = {
    var args = inputArgs.toList
    var currentBuilder = this
    while (args.nonEmpty) {
      currentBuilder = args match {
        case (KUBERNETES_APP_NAME) :: value :: tail =>
          args = tail
          currentBuilder.kubernetesAppName(value)

        case (CLASS) :: value :: tail =>
          args = tail
          currentBuilder.userMainClass(value)

        case (CONF) :: value :: tail =>
          args = tail
          currentBuilder.addSparkConf(value.split("=")(0), value.split("=")(1))

        case (JARS) :: value :: tail =>
          args = tail
          for (jar <- value.split(",")) {
            currentBuilder = currentBuilder.addJar(jar)
          }
          currentBuilder

        case "--arg" :: value :: tail =>
          args = tail
          currentBuilder.addUserArg(value)

        case (KUBERNETES_EXPOSE_DRIVER_PORT) :: value :: tail =>
          args = tail
          currentBuilder.addDriverPort(
            value.split("=")(0), value.split("=")(1).toInt)

        case (DRIVER_CLASS_PATH) :: value :: tail =>
          args = tail
          for (entry <- value.split(",")) {
            currentBuilder = currentBuilder.addDriverExtraClassPath(entry)
          }
          currentBuilder

        case (KUBERNETES_DRIVER_DOCKER_IMAGE) :: value :: tail =>
          args = tail
          currentBuilder.driverDockerImage(value)

        case (KUBERNETES_EXECUTOR_DOCKER_IMAGE) :: value :: tail =>
          args = tail
          currentBuilder.executorDockerImage(value)

        case (KUBERNETES_CUSTOM_EXECUTOR_SPEC_FILE) :: value :: tail =>
          args = tail
          currentBuilder.customExecutorSpecFile(value)

        case (KUBERNETES_CUSTOM_EXECUTOR_SPEC_CONTAINER_NAME) :: value :: tail =>
          args = tail
          currentBuilder.customExecutorSpecContainerName(value)

        case (KUBERNETES_MASTER) :: value :: tail =>
          args = tail
          currentBuilder.kubernetesMaster(value)

        case (KUBERNETES_APP_NAMESPACE) :: value :: tail =>
          args = tail
          currentBuilder.kubernetesAppNamespace(value)

        case (KUBERNETES_CA_CERT_FILE) :: value :: tail =>
          args = tail
          currentBuilder.kubernetesCaCertFile(value)

        case (KUBERNETES_CLIENT_CERT_FILE) :: value :: tail =>
          args = tail
          currentBuilder.kubernetesClientCertFile(value)

        case (KUBERNETES_CLIENT_KEY_FILE) :: value :: tail =>
          args = tail
          currentBuilder.kubernetesClientKeyFile(value)

        case Nil => currentBuilder

        case _ =>
          throw new IllegalArgumentException(getUsageMessage(args))
      }
    }
    currentBuilder
  }

  def build(): ClientArguments = {
    val withSystemProperties = withLoadedFromSparkConf()
    validateOptions(
      (withSystemProperties.userMainClass, "Must specify a main class to run"),
      (withSystemProperties.kubernetesMaster, "Must specify a Kubernetes master."))
    ClientArguments(
      kubernetesAppName = withSystemProperties.kubernetesAppName,
      userMainClass = withSystemProperties.userMainClass.get,
      userArgs = withSystemProperties.userArgs.toArray,
      jars = withSystemProperties.jars.toArray,
      additionalDriverPorts = withSystemProperties.additionalDriverPorts,
      sparkConf = withSystemProperties.sparkConf,
      driverExtraClassPath = withSystemProperties.driverExtraClassPath.toArray,
      // TODO use specific versions
      driverDockerImage = withSystemProperties.driverDockerImage.getOrElse("spark-driver:latest"),
      executorDockerImage = withSystemProperties
        .executorDockerImage
        .getOrElse("spark-executor:latest"),
      customExecutorSpecFile = withSystemProperties.customExecutorSpecFile,
      customExecutorSpecContainerName = withSystemProperties.customExecutorSpecContainerName,
      kubernetesMaster = withSystemProperties.kubernetesMaster.get,
      kubernetesAppNamespace = withSystemProperties.kubernetesAppNamespace.getOrElse("default"),
      kubernetesClientCertFile = withSystemProperties.kubernetesClientCertFile,
      kubernetesClientKeyFile = withSystemProperties.kubernetesClientKeyFile,
      kubernetesCaCertFile = withSystemProperties.kubernetesCaCertFile)
  }

  private def withLoadedFromSparkConf(): ClientArgumentsBuilder = {
    val sysPropsSparkConf = new SparkConf(true)
    val sparkConfKubernetesAppName = sysPropsSparkConf.getOption("spark.kubernetes.app.name")
    val sparkConfKubernetesMaster = sysPropsSparkConf.getOption("spark.kubernetes.master")
    val sparkConfKubernetesNamespace = sysPropsSparkConf.getOption("spark.kubernetes.namespace")
    val sparkConfKubernetesClientCertFile = sysPropsSparkConf
      .getOption("spark.kubernetes.client.cert.file")
    val sparkConfKubernetesClientKeyFile = sysPropsSparkConf
      .getOption("spark.kubernetes.client.key.file")
    val sparkConfKubernetesCaCertFile = sysPropsSparkConf.getOption("spark.kubernetes.ca.cert.file")
    val sparkConfExecutorCustomSpecFile = sysPropsSparkConf
      .getOption("spark.kubernetes.executor.custom.spec.file")
    val sparkConfExecutorCustomSpecContainerName = sysPropsSparkConf
      .getOption("spark.kubernetes.executor.custom.spec.container.name")
    val sparkConfExecutorDockerImage = sysPropsSparkConf
      .getOption("spark.kubernetes.executor.docker.image")
    val resolvedSparkConf = sysPropsSparkConf.getAll.toMap ++ sparkConf
    val resolvedJars = if (jars.isEmpty) {
      sysPropsSparkConf
        .getOption("spark.jars")
        .map(_.split(",").toSeq)
        .getOrElse(Seq.empty[String])
    } else {
      jars
    }
    val resolvedDriverExtraClassPath = if (driverExtraClassPath.isEmpty) {
      sysPropsSparkConf
        .getOption("spark.driver.extraClassPath")
        .map(_.split(",").toSeq)
        .getOrElse(Seq.empty[String])
    } else {
      driverExtraClassPath
    }
    copy(kubernetesAppName = kubernetesAppName.orElse(sparkConfKubernetesAppName),
      jars = resolvedJars,
      driverExtraClassPath = resolvedDriverExtraClassPath,
      executorDockerImage = executorDockerImage.orElse(sparkConfExecutorDockerImage),
      kubernetesMaster = kubernetesMaster.orElse(sparkConfKubernetesMaster),
      kubernetesAppNamespace = kubernetesAppNamespace.orElse(sparkConfKubernetesNamespace),
      sparkConf = resolvedSparkConf.toMap,
      customExecutorSpecFile = customExecutorSpecFile.orElse(sparkConfExecutorCustomSpecFile),
      customExecutorSpecContainerName = customExecutorSpecContainerName
        .orElse(sparkConfExecutorCustomSpecContainerName),
      kubernetesClientCertFile = kubernetesClientCertFile.orElse(sparkConfKubernetesClientCertFile),
      kubernetesClientKeyFile = kubernetesClientKeyFile.orElse(sparkConfKubernetesClientKeyFile),
      kubernetesCaCertFile = kubernetesClientCertFile.orElse(sparkConfKubernetesCaCertFile))
  }

  private def validateOptions(opts: (Option[String], String)*): Unit = {
    opts.foreach(opt => assert(opt._1.isDefined, opt._2))
  }

  private def getUsageMessage(unknownParam: List[String] = null): String = {
    if (unknownParam != null) s"Unknown/unsupported param $unknownParam\n" else ""
  }
}
