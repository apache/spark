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

package org.apache.spark.deploy.k8s.features

import java.net.URL
import java.util.UUID

import scala.io.Source

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, ContainerBuilder, HasMetadata, PodBuilder, VolumeMountBuilder}

import org.apache.spark.deploy.k8s.{Config, KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.launcher.SparkLauncher.{DRIVER_EXTRA_JAVA_OPTIONS, EXECUTOR_EXTRA_JAVA_OPTIONS}

/**
 * Mounts the logger configuration from local configuration directory -
 * on the spark job submitter's end or from a pre-defined config map.
 */
class MountLogConfFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep with Logging {

  private val useExistingConfigMap = conf.get(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP).isDefined
  private val configMapName: String = conf.get(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP)
    .getOrElse(s"config-map-logger-${UUID.randomUUID().toString.take(3)}")
  private val loggingConfigFileName: String = conf.get(Config.KUBERNETES_LOGGING_CONF_FILE_NAME)
  private val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)
  private val loggerJVMProp =
    s"${MountLogConfFeatureStep.JAVA_OPT_FOR_LOGGING}${loggingConfigFileName}"

  private val featureEnabled: Boolean = {
    (loggingConfURL != null &&
      conf.getOption(SparkLauncher.DEPLOY_MODE).getOrElse("client")
        .equalsIgnoreCase("cluster")) ||
      useExistingConfigMap
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    val logConfVolume = s"log-conf-vol-${UUID.randomUUID().toString.take(3)}"
    if (useExistingConfigMap) {
      logInfo(s"Using an existing config map ${configMapName} for logging configuration.")
    }

    val podUpdated = if (featureEnabled) {
      new PodBuilder(pod.pod)
      .editSpec()
        .addNewVolume()
          .withName(logConfVolume)
          .withNewConfigMap()
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()
    } else {
      logDebug(s"Logging configuration mount not performed.")
      pod.pod
    }

    val containerUpdated =
      if (featureEnabled) {
        new ContainerBuilder(pod.container)
            .addNewVolumeMount()
            .withName(logConfVolume)
              .withMountPath(LOGGING_MOUNT_DIR)
              .endVolumeMount()
          .build()
      } else {
        pod.container
      }
    SparkPod(podUpdated, containerUpdated)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    if (featureEnabled) {
      val executorJavaOpts =
        s"$loggerJVMProp ${conf.get(EXECUTOR_EXTRA_JAVA_OPTIONS, "")}"
      val driverJavaOpts =
        s"$loggerJVMProp ${conf.get(DRIVER_EXTRA_JAVA_OPTIONS, "")}"
      Map((EXECUTOR_EXTRA_JAVA_OPTIONS -> executorJavaOpts),
        (DRIVER_EXTRA_JAVA_OPTIONS -> driverJavaOpts),
        (Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP.key -> configMapName))
    } else {
      Map.empty[String, String]
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (featureEnabled && !useExistingConfigMap) {
      Seq(MountLogConfFeatureStep
        .buildConfigMap(loggingConfURL, loggingConfigFileName, configMapName))
    } else {
      Seq[HasMetadata]()
    }
  }
}

private[k8s] object MountLogConfFeatureStep extends Logging {
  // Logging configuration for containers.
  val JAVA_OPT_FOR_LOGGING = s"-Dlog4j.configuration=file://$LOGGING_MOUNT_DIR/"

  // exposed for testing.
  private[k8s] def buildConfigMap(
      loggingConfUrl: URL, loggingConfigFileName: String, configMapName: String): ConfigMap = {
    logInfo(s"Logging configuration is picked up from: $loggingConfigFileName")
    val loggerConfStream = loggingConfUrl.openStream()
    val loggerConfString = Source.createBufferedSource(loggerConfStream).getLines().mkString("\n")
    new ConfigMapBuilder()
      .withNewMetadata()
      .withName(configMapName)
      .endMetadata()
      .addToData(loggingConfigFileName, loggerConfString)
      .build()
  }
}
