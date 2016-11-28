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
package org.apache.spark.deploy.kubernetes.shuffle

import java.util.UUID

import io.fabric8.kubernetes.api.model.QuantityBuilder
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}
import org.apache.commons.lang3.Validate
import scala.collection.JavaConverters._

import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.SPARK_VERSION
import org.apache.spark.util.Utils

private[spark] case class StartKubernetesShuffleServiceArguments(
  kubernetesMaster: String,
  shuffleServiceNamespace: String,
  shuffleServiceDaemonSetName: String,
  shuffleServicePort: Int,
  shuffleServiceMemory: String,
  shuffleServiceDockerImage: String,
  shuffleHostPathDir: String,
  kubernetesClientCertFile: Option[String] = None,
  kubernetesClientKeyFile: Option[String] = None,
  kubernetesCaCertFile: Option[String] = None)

private[spark] case class StartKubernetesShuffleServiceArgumentsBuilder(
  kubernetesMaster: Option[String] = None,
  shuffleServiceNamespace: Option[String] = None,
  shuffleServiceDaemonSetName: String = "spark-shuffle-service",
  shuffleServicePort: Int = 7337,
  shuffleServiceMemory: String = "1g",
  shuffleHostPathDir: String = "/tmp",
  shuffleServiceDockerImage: String = s"spark-shuffle-service:$SPARK_VERSION",
  kubernetesClientCertFile: Option[String] = None,
  kubernetesClientKeyFile: Option[String] = None,
  kubernetesCaCertFile: Option[String] = None) {

  def build(): StartKubernetesShuffleServiceArguments = {
    val resolvedMaster = kubernetesMaster.getOrElse(
      throw new IllegalArgumentException("Must specify kubernetes master"))
    val resolvedNamespace = shuffleServiceNamespace.getOrElse(
      throw new IllegalArgumentException("Must specify namespace"))
    StartKubernetesShuffleServiceArguments(
      kubernetesMaster = resolvedMaster,
      shuffleServiceNamespace = resolvedNamespace,
      shuffleServiceDaemonSetName = shuffleServiceDaemonSetName,
      shuffleServicePort = shuffleServicePort,
      shuffleServiceMemory = shuffleServiceMemory,
      shuffleHostPathDir = shuffleHostPathDir,
      shuffleServiceDockerImage = shuffleServiceDockerImage,
      kubernetesClientCertFile = kubernetesClientCertFile,
      kubernetesClientKeyFile = kubernetesClientKeyFile,
      kubernetesCaCertFile = kubernetesCaCertFile)
  }
}

private[spark] object StartKubernetesShuffleServiceArgumentsBuilder
    extends SparkSubmitArgumentsParser {

  def fromArgsArray(argStrings: Array[String]): StartKubernetesShuffleServiceArguments = {
    var args = argStrings.toList
    var currentBuilder = new StartKubernetesShuffleServiceArgumentsBuilder()
    while (args.nonEmpty) {
      currentBuilder = args match {
        case "--kubernetes-master" :: value :: tail =>
          args = tail
          currentBuilder.copy(
            kubernetesMaster = Some(Validate.notBlank(value,
              "Kubernetes master must not be empty.")))
        case "--namespace" :: value :: tail =>
          args = tail
          currentBuilder.copy(
            shuffleServiceNamespace = Some(Validate.notBlank(value,
              "Namespace must not be empty.")))
        case "--daemon-set-name" :: value :: tail =>
          args = tail
          currentBuilder.copy(
            shuffleServiceDaemonSetName = Validate.notBlank(value,
              "Daemon set name must not be empty."))
        case "--port" :: value :: tail =>
          args = tail
          currentBuilder.copy(
            shuffleServicePort = Validate.notBlank(value, "Port must not be empty.").toInt)
        case "--memory" :: value :: tail =>
          args = tail
          currentBuilder.copy(
            shuffleServiceMemory = Validate.notBlank(value, "Memory must not be empty."))
        case "--shuffle-service-host-directory" :: value :: tail =>
          args = tail
          currentBuilder.copy(shuffleHostPathDir = Validate.notBlank(value,
            "Shuffle host directory must not be empty."))
        case KUBERNETES_CLIENT_KEY_FILE :: value :: tail =>
          args = tail
          currentBuilder.copy(
            kubernetesClientKeyFile = Some(Validate.notBlank(value,
              "Client key file must not be empty.")))
        case KUBERNETES_CLIENT_CERT_FILE :: value :: tail =>
          args = tail
          currentBuilder.copy(
            kubernetesClientCertFile = Some(Validate.notBlank(value,
              "Client cert file must not be empty.")))
        case KUBERNETES_CA_CERT_FILE :: value :: tail =>
          args = tail
          currentBuilder.copy(
            kubernetesCaCertFile = Some(Validate.notBlank(value,
              "Ca cert file must not be empty.")))
        case Nil => currentBuilder
        case _ =>
          // TODO fill in usage message
          throw new IllegalArgumentException("Unsupported parameter")
      }
    }
    currentBuilder.build()
  }
}
private[spark] class StartKubernetesShuffleService {
  import StartKubernetesShuffleService._

  def run(parsedArguments: StartKubernetesShuffleServiceArguments): Unit = {
    var clientConfigBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(parsedArguments.kubernetesMaster)
      .withNamespace(parsedArguments.shuffleServiceNamespace)

    parsedArguments.kubernetesCaCertFile.foreach(path => {
      clientConfigBuilder = clientConfigBuilder.withCaCertFile(path)
    })
    parsedArguments.kubernetesClientCertFile.foreach(path => {
      clientConfigBuilder = clientConfigBuilder.withClientCertFile(path)
    })
    parsedArguments.kubernetesClientKeyFile.foreach(path => {
      clientConfigBuilder = clientConfigBuilder.withClientKeyFile(path)
    })
    Utils.tryWithResource(new DefaultKubernetesClient(clientConfigBuilder.build())) {
      kubernetesClient =>
        val shuffleServiceLabel = UUID.randomUUID.toString.replaceAll("-", "")
        val selectors = Map(SHUFFLE_SERVICE_SELECTOR -> shuffleServiceLabel).asJava
        val requestedShuffleServiceMemoryBytes = Utils.byteStringAsBytes(
          parsedArguments.shuffleServiceMemory).toString
        val shuffleServiceMemoryQuantity = new QuantityBuilder(false)
          .withAmount(requestedShuffleServiceMemoryBytes)
          .build()
        kubernetesClient.extensions().daemonSets().createNew()
          .withNewMetadata()
            .withName(parsedArguments.shuffleServiceDaemonSetName)
            .withLabels(selectors)
            .endMetadata()
          .withNewSpec()
            .withNewSelector()
            .withMatchLabels(selectors)
            .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withName(parsedArguments.shuffleServiceDaemonSetName)
              .withLabels(selectors)
              .endMetadata()
            .withNewSpec()
              .addNewVolume()
                .withName("shuffles-volume")
                .withNewHostPath().withPath(parsedArguments.shuffleHostPathDir).endHostPath()
                .endVolume()
              .addNewContainer()
                .withName(s"shuffle-service-container")
                .withImage(parsedArguments.shuffleServiceDockerImage)
                .withImagePullPolicy("IfNotPresent")
                .addNewPort().withContainerPort(parsedArguments.shuffleServicePort).endPort()
                .addNewEnv()
                  .withName("SPARK_SHUFFLE_SERVICE_PORT")
                  .withValue(parsedArguments.shuffleServicePort.toString)
                  .endEnv()
                .addNewEnv()
                  .withName("SPARK_SHUFFLE_SERVICE_MEMORY")
                  .withValue(parsedArguments.shuffleServiceMemory)
                  .endEnv()
                .withNewResources()
                  .addToRequests("memory", shuffleServiceMemoryQuantity)
                  .endResources()
                .addNewVolumeMount()
                  .withName("shuffles-volume")
                  .withMountPath(parsedArguments.shuffleHostPathDir)
                  .withReadOnly(true)
                  .endVolumeMount()
                .endContainer()
              .endSpec()
            .endTemplate()
          .endSpec()
        .done()
    }
  }
}

private[spark] object StartKubernetesShuffleService {
  private val SHUFFLE_SERVICE_SELECTOR = "spark-shuffle-service"
  def main(args: Array[String]): Unit = {
    val parsedArguments = StartKubernetesShuffleServiceArgumentsBuilder.fromArgsArray(args)
    new StartKubernetesShuffleService().run(parsedArguments)
  }
}
