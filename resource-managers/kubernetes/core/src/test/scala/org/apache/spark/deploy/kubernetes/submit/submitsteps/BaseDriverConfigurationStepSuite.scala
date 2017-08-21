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
package org.apache.spark.deploy.kubernetes.submit.submitsteps

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

private[spark] class BaseDriverConfigurationStepSuite extends SparkFunSuite {

  private val APP_ID = "spark-app-id"
  private val RESOURCE_NAME_PREFIX = "spark"
  private val DRIVER_LABELS = Map("labelkey" -> "labelvalue")
  private val DOCKER_IMAGE_PULL_POLICY = "IfNotPresent"
  private val APP_NAME = "spark-test"
  private val MAIN_CLASS = "org.apache.spark.examples.SparkPi"
  private val APP_ARGS = Array("arg1", "arg2")
  private val CUSTOM_ANNOTATION_KEY = "customAnnotation"
  private val CUSTOM_ANNOTATION_VALUE = "customAnnotationValue"
  private val DEPRECATED_CUSTOM_ANNOTATION_KEY = "customAnnotationDeprecated"
  private val DEPRECATED_CUSTOM_ANNOTATION_VALUE = "customAnnotationDeprecatedValue"
  private val DRIVER_CUSTOM_ENV_KEY1 = "customDriverEnv1"
  private val DRIVER_CUSTOM_ENV_KEY2 = "customDriverEnv2"

  test("Set all possible configurations from the user.") {
    val sparkConf = new SparkConf()
        .set(KUBERNETES_DRIVER_POD_NAME, "spark-driver-pod")
        .set(org.apache.spark.internal.config.DRIVER_CLASS_PATH, "/opt/spark/spark-exmaples.jar")
        .set("spark.driver.cores", "2")
        .set(KUBERNETES_DRIVER_LIMIT_CORES, "4")
        .set(org.apache.spark.internal.config.DRIVER_MEMORY, 256L)
        .set(KUBERNETES_DRIVER_MEMORY_OVERHEAD, 200L)
        .set(DRIVER_DOCKER_IMAGE, "spark-driver:latest")
        .set(s"spark.kubernetes.driver.annotation.$CUSTOM_ANNOTATION_KEY", CUSTOM_ANNOTATION_VALUE)
        .set("spark.kubernetes.driver.annotations",
            s"$DEPRECATED_CUSTOM_ANNOTATION_KEY=$DEPRECATED_CUSTOM_ANNOTATION_VALUE")
        .set(s"$KUBERNETES_DRIVER_ENV_KEY$DRIVER_CUSTOM_ENV_KEY1", "customDriverEnv1")
        .set(s"$KUBERNETES_DRIVER_ENV_KEY$DRIVER_CUSTOM_ENV_KEY2", "customDriverEnv2")

    val submissionStep = new BaseDriverConfigurationStep(
        APP_ID,
        RESOURCE_NAME_PREFIX,
        DRIVER_LABELS,
        DOCKER_IMAGE_PULL_POLICY,
        APP_NAME,
        MAIN_CLASS,
        APP_ARGS,
        sparkConf)
    val basePod = new PodBuilder().withNewMetadata().endMetadata().withNewSpec().endSpec().build()
    val baseDriverSpec = KubernetesDriverSpec(
        driverPod = basePod,
        driverContainer = new ContainerBuilder().build(),
        driverSparkConf = new SparkConf(false),
        otherKubernetesResources = Seq.empty[HasMetadata])

    val preparedDriverSpec = submissionStep.configureDriver(baseDriverSpec)
    assert(preparedDriverSpec.driverContainer.getName === DRIVER_CONTAINER_NAME)
    assert(preparedDriverSpec.driverContainer.getImage === "spark-driver:latest")
    assert(preparedDriverSpec.driverContainer.getImagePullPolicy === DOCKER_IMAGE_PULL_POLICY)
    val envs = preparedDriverSpec.driverContainer
        .getEnv
        .asScala
        .map(env => (env.getName, env.getValue))
        .toMap
    assert(envs.size === 6)
    assert(envs(ENV_SUBMIT_EXTRA_CLASSPATH) === "/opt/spark/spark-exmaples.jar")
    assert(envs(ENV_DRIVER_MEMORY) === "456m")
    assert(envs(ENV_DRIVER_MAIN_CLASS) === MAIN_CLASS)
    assert(envs(ENV_DRIVER_ARGS) === "arg1 arg2")
    assert(envs(DRIVER_CUSTOM_ENV_KEY1) === "customDriverEnv1")
    assert(envs(DRIVER_CUSTOM_ENV_KEY2) === "customDriverEnv2")
    val resourceRequirements = preparedDriverSpec.driverContainer.getResources
    val requests = resourceRequirements.getRequests.asScala
    assert(requests("cpu").getAmount === "2")
    assert(requests("memory").getAmount === "256M")
    val limits = resourceRequirements.getLimits.asScala
    assert(limits("memory").getAmount === "456M")
    assert(limits("cpu").getAmount === "4")
    val driverPodMetadata = preparedDriverSpec.driverPod.getMetadata
    assert(driverPodMetadata.getName === "spark-driver-pod")
    assert(driverPodMetadata.getLabels.asScala === DRIVER_LABELS)
    val expectedAnnotations = Map(
      CUSTOM_ANNOTATION_KEY -> CUSTOM_ANNOTATION_VALUE,
      DEPRECATED_CUSTOM_ANNOTATION_KEY -> DEPRECATED_CUSTOM_ANNOTATION_VALUE,
      SPARK_APP_NAME_ANNOTATION -> APP_NAME)
    assert(driverPodMetadata.getAnnotations.asScala === expectedAnnotations)
    assert(preparedDriverSpec.driverPod.getSpec.getRestartPolicy === "Never")
    val resolvedSparkConf = preparedDriverSpec.driverSparkConf.getAll.toMap
    val expectedSparkConf = Map(
      KUBERNETES_DRIVER_POD_NAME.key -> "spark-driver-pod",
      "spark.app.id" -> APP_ID,
      KUBERNETES_EXECUTOR_POD_NAME_PREFIX.key -> RESOURCE_NAME_PREFIX)
    assert(resolvedSparkConf === expectedSparkConf)

  }
}
