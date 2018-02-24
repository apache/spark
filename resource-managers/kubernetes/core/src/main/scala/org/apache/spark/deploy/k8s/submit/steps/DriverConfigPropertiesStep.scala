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
package org.apache.spark.deploy.k8s.submit.steps

import java.io.StringWriter
import java.util.Properties

import io.fabric8.kubernetes.api.model._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec

/**
 * Create a config map with the driver configuration and attach it to the pod. This needs to
 * come at the end of the driver configuration so that all modifications to the Spark config
 * are reflected in the generated config map.
 */
private[spark] class DriverConfigPropertiesStep(resourceNamePrefix: String)
    extends DriverConfigurationStep {

  override def configureDriver(spec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val configMapName = s"$resourceNamePrefix-driver-conf-map"
    val configMap = buildConfigMap(configMapName, spec.driverSparkConf)

    val configMountedPod = new PodBuilder(spec.driverPod)
      .editSpec()
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME)
          .withNewConfigMap()
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()

    val volumeMountedContainer = new ContainerBuilder(spec.driverContainer)
      .addNewVolumeMount()
        .withName(SPARK_CONF_VOLUME)
        .withMountPath(SPARK_CONF_DIR)
        .endVolumeMount()
      .build()

    val resourcesWithDriverConfigMap = spec.otherKubernetesResources ++ Seq(configMap)

    spec.copy(
      driverPod = configMountedPod,
      driverContainer = volumeMountedContainer,
      otherKubernetesResources = resourcesWithDriverConfigMap)
  }

  private def buildConfigMap(configMapName: String, conf: SparkConf): ConfigMap = {
    val properties = new Properties()
    conf.getAll.foreach { case (k, v) =>
      properties.setProperty(k, v)
    }
    val propertiesWriter = new StringWriter()
    properties.store(propertiesWriter,
      s"Java properties built from Kubernetes config map with name: $configMapName")

    val namespace = conf.get(KUBERNETES_NAMESPACE)
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .withNamespace(namespace)
        .endMetadata()
      .addToData(SPARK_CONF_FILE_NAME, propertiesWriter.toString)
      .build()
  }
}
