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

import scala.collection.JavaConverters._
import scala.io.Source

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, ContainerBuilder, EnvVar, EnvVarBuilder, HasMetadata, PodBuilder, VolumeMountBuilder}

import org.apache.spark.deploy.k8s.{Config, KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher.{DRIVER_EXTRA_JAVA_OPTIONS, EXECUTOR_EXTRA_JAVA_OPTIONS}

/**
 * Mounts the logger configuration from local configuration directory -
 * on the spark job submitter's end or from a pre-defined config map.
 */
class MountLogConfFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep with Logging {
  private val useExistingConfigMap = conf.get(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP).isDefined
  private val configMapName: String = conf.get(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP)
    .getOrElse(s"config-map-logging-conf-${UUID.randomUUID().toString.take(5)}")
  private val loggingConfigFileName: String = conf.get(Config.KUBERNETES_LOGGING_CONF_FILE_NAME)
  private val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)

  private val featureEnabled: Boolean = {
      (loggingConfURL != null || useExistingConfigMap)
  }
  private val loggerJVMProp = s"$JAVA_OPT_FOR_LOGGING${loggingConfigFileName}"

  override def configurePod(pod: SparkPod): SparkPod = {
    val logConfVolume = s"logger-conf-volume-${UUID.randomUUID().toString.take(5)}"
    if (useExistingConfigMap) {
      logInfo(s"Using an existing config map ${configMapName} for logging configuration.")
    }
    // Existing value of SPARK_CLASSPATH environment variable.
    val sparkClasspath =
      pod.container.getEnv.asScala.find(p => p.getName == ENV_CLASSPATH).map {x => x.getValue}
    // All other environment variable except SPARK_CLASSPATH.
    val envVars = pod.container.getEnv.asScala.filterNot(p => p.getName == ENV_CLASSPATH)
    // Update the classpath with path to logger configuration file.
    val updatedClasspath = if (sparkClasspath.isDefined) {
      s"$LOGGING_MOUNT_DIR:${sparkClasspath.get}"
    } else {
      LOGGING_MOUNT_DIR
    }
    val sparkClasspathEnv =
      new EnvVarBuilder().withName(ENV_CLASSPATH).withValue(updatedClasspath).build()
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
      logDebug(s"Logging configuration not found, mount not performed.")
      pod.pod
    }

    val configMapVolumeMount = new VolumeMountBuilder()
        .withName(logConfVolume)
        .withReadOnly(true)
      // We need a separate mounting dir for logging because,
      // Mounting a ConfigMap has limitation that the mounted directory can hold only 1 file.
        .withMountPath(LOGGING_MOUNT_DIR)
        .build()
    // As per https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.14/
    // #configmapvolumesource-v1-core
    // Since we do not want the pod creation to fail, in absence of the config map.
    configMapVolumeMount.setAdditionalProperty("optional", "true")

    val containerUpdated =
      if (featureEnabled) {
        new ContainerBuilder(pod.container)
          .withEnv((envVars ++ Seq(sparkClasspathEnv)).asJava)
          .withVolumeMounts(configMapVolumeMount)
          .build()
      } else {
        pod.container
      }
    SparkPod(podUpdated, containerUpdated)
  }

  private def buildConfigMap(configMapName: String): ConfigMap = {
    val loggerConfStream =
      this.getClass.getClassLoader.getResourceAsStream(loggingConfigFileName)
    logInfo(s"Logging configuration is picked up from: $loggingConfURL")
    val loggerConfString = Source.createBufferedSource(loggerConfStream).getLines().mkString("\n")
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .endMetadata()
      .addToData(loggingConfigFileName, loggerConfString)
      .build()
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
      Seq(buildConfigMap(configMapName))
    } else {
      Seq[HasMetadata]()
    }
  }
}
