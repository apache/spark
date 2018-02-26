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

import java.io.StringReader
import java.util.Properties

import scala.collection.JavaConverters._

import com.google.common.collect.Maps
import io.fabric8.kubernetes.api.model.{ConfigMap, ContainerBuilder, HasMetadata, PodBuilder, Volume, VolumeMount}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec
import org.apache.spark.util.Utils

class DriverConfigPropertiesStepSuite extends SparkFunSuite {

  private val RESOURCE_NAME_PREFIX = "spark"
  private val NAMESPACE = "test-namepsace"
  private val CUSTOM_JAVA_PROPERTY_KEY = "customJavaPropKey"
  private val CUSTOM_JAVA_PROPERTY_VALUE = "customJavaPropValue"
  private val POD_NAME = "example-pod-name"
  private val CONTAINER_NAME = "example-container-name"

  test("Testing driver configuration config map mounting") {
    val sparkConf = new SparkConf(false)
      .set(CUSTOM_JAVA_PROPERTY_KEY, CUSTOM_JAVA_PROPERTY_VALUE)
      .set(KUBERNETES_NAMESPACE, NAMESPACE)
    val submissionStep = new DriverConfigPropertiesStep(RESOURCE_NAME_PREFIX)
    val basePod = new PodBuilder()
      .withNewMetadata()
        .withName(POD_NAME)
      .endMetadata()
      .withNewSpec()
      .endSpec()
      .build()
    val baseDriverSpec = KubernetesDriverSpec(
      driverPod = basePod,
      driverContainer = new ContainerBuilder().withName(CONTAINER_NAME).build(),
      driverSparkConf = sparkConf,
      otherKubernetesResources = Seq.empty[HasMetadata])
    val preparedDriverSpec = submissionStep.configureDriver(baseDriverSpec)
    assert(preparedDriverSpec.otherKubernetesResources.size === 1)
    val EXPECTED_CONFIG_MAP_NAME = s"$RESOURCE_NAME_PREFIX-driver-conf-map"
    assert(preparedDriverSpec.otherKubernetesResources.exists {
      case configMap: ConfigMap =>
        val hasMatchingName = configMap.getMetadata.getName == EXPECTED_CONFIG_MAP_NAME
        val hasMatchingNamespace =
          configMap.getMetadata.getNamespace == NAMESPACE
        val configMapData = configMap.getData.asScala
        val hasCorrectNumberOfEntries = configMapData.size == 1
        val driverPropertiesRaw = configMapData(SPARK_CONF_FILE_NAME)
        val driverProperties = new Properties()
        Utils.tryWithResource(new StringReader(driverPropertiesRaw)) {
          driverProperties.load(_)
        }
        val driverPropertiesMap = Maps.fromProperties(driverProperties).asScala
        val expectedDriverProperties = Map(
          CUSTOM_JAVA_PROPERTY_KEY -> CUSTOM_JAVA_PROPERTY_VALUE,
          "spark.kubernetes.namespace" -> NAMESPACE)
        val hasMatchingProperties = driverPropertiesMap == expectedDriverProperties
        hasMatchingName && hasMatchingNamespace &&
          hasCorrectNumberOfEntries && hasMatchingProperties
      case _ => false
    })
    assert(preparedDriverSpec.driverPod.getSpec.getVolumes.toArray().exists {
      case volume: Volume =>
        val hasMatchingVolumeName = volume.getName == SPARK_CONF_VOLUME
        val hasConfigMap = volume.getConfigMap.getName == EXPECTED_CONFIG_MAP_NAME
        hasMatchingVolumeName && hasConfigMap
      case _ => false
    })
    assert(preparedDriverSpec.driverContainer.getVolumeMounts.toArray().exists {
      case volumeMount: VolumeMount =>
        val hasMatchingVolumeMountName = volumeMount.getName == SPARK_CONF_VOLUME
        val hasVolumeMountPath = volumeMount.getMountPath == SPARK_CONF_DIR
        hasMatchingVolumeMountName && hasVolumeMountPath
      case _ => false
    })
    assert(preparedDriverSpec.driverSparkConf.getAll.sameElements(sparkConf.getAll))
    assert(preparedDriverSpec.driverPod.getMetadata.getName == POD_NAME)
    assert(preparedDriverSpec.driverContainer.getName == CONTAINER_NAME)
  }
}
