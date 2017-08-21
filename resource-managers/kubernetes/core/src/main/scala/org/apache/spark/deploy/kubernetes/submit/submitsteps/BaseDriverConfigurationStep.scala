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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, PodBuilder, QuantityBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.ConfigurationUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

/**
 * Represents the initial setup required for the driver.
 */
private[spark] class BaseDriverConfigurationStep(
    kubernetesAppId: String,
    kubernetesResourceNamePrefix: String,
    driverLabels: Map[String, String],
    dockerImagePullPolicy: String,
    appName: String,
    mainClass: String,
    appArgs: Array[String],
    submissionSparkConf: SparkConf) extends DriverConfigurationStep {

  private val kubernetesDriverPodName = submissionSparkConf.get(KUBERNETES_DRIVER_POD_NAME)
      .getOrElse(s"$kubernetesResourceNamePrefix-driver")
  private val driverExtraClasspath = submissionSparkConf.get(
      org.apache.spark.internal.config.DRIVER_CLASS_PATH)
  // CPU settings
  private val driverCpuCores = submissionSparkConf.getOption("spark.driver.cores").getOrElse("1")
  private val driverLimitCores = submissionSparkConf.get(KUBERNETES_DRIVER_LIMIT_CORES)

  // Memory settings
  private val driverMemoryMb = submissionSparkConf.get(
      org.apache.spark.internal.config.DRIVER_MEMORY)
  private val memoryOverheadMb = submissionSparkConf
      .get(KUBERNETES_DRIVER_MEMORY_OVERHEAD)
      .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * driverMemoryMb).toInt,
          MEMORY_OVERHEAD_MIN))
  private val driverContainerMemoryWithOverhead = driverMemoryMb + memoryOverheadMb
  private val driverDockerImage = submissionSparkConf.get(DRIVER_DOCKER_IMAGE)

  override def configureDriver(
      driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val driverExtraClasspathEnv = driverExtraClasspath.map { classPath =>
      new EnvVarBuilder()
        .withName(ENV_SUBMIT_EXTRA_CLASSPATH)
        .withValue(classPath)
        .build()
    }
    val driverCustomAnnotations = ConfigurationUtils
        .combinePrefixedKeyValuePairsWithDeprecatedConf(
            submissionSparkConf,
            KUBERNETES_DRIVER_ANNOTATION_PREFIX,
            KUBERNETES_DRIVER_ANNOTATIONS,
            "annotation")
    require(!driverCustomAnnotations.contains(SPARK_APP_NAME_ANNOTATION),
        s"Annotation with key $SPARK_APP_NAME_ANNOTATION is not allowed as it is reserved for" +
            s" Spark bookkeeping operations.")

    val driverCustomEnvs = submissionSparkConf.getAllWithPrefix(KUBERNETES_DRIVER_ENV_KEY).toSeq
      .map(env => new EnvVarBuilder()
        .withName(env._1)
        .withValue(env._2)
        .build())

    val allDriverAnnotations = driverCustomAnnotations ++ Map(SPARK_APP_NAME_ANNOTATION -> appName)
    val nodeSelector = ConfigurationUtils.parsePrefixedKeyValuePairs(
      submissionSparkConf, KUBERNETES_NODE_SELECTOR_PREFIX, "node selector")
    val driverCpuQuantity = new QuantityBuilder(false)
      .withAmount(driverCpuCores)
      .build()
    val driverMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverMemoryMb}M")
      .build()
    val driverMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverContainerMemoryWithOverhead}M")
      .build()
    val maybeCpuLimitQuantity = driverLimitCores.map { limitCores =>
      ("cpu", new QuantityBuilder(false).withAmount(limitCores).build())
    }
    val driverContainer = new ContainerBuilder(driverSpec.driverContainer)
      .withName(DRIVER_CONTAINER_NAME)
      .withImage(driverDockerImage)
      .withImagePullPolicy(dockerImagePullPolicy)
      .addAllToEnv(driverCustomEnvs.asJava)
      .addToEnv(driverExtraClasspathEnv.toSeq: _*)
      .addNewEnv()
        .withName(ENV_DRIVER_MEMORY)
        .withValue(driverContainerMemoryWithOverhead + "m")
        .endEnv()
      .addNewEnv()
        .withName(ENV_DRIVER_MAIN_CLASS)
        .withValue(mainClass)
        .endEnv()
      .addNewEnv()
        .withName(ENV_DRIVER_ARGS)
        .withValue(appArgs.mkString(" "))
        .endEnv()
      .withNewResources()
        .addToRequests("cpu", driverCpuQuantity)
        .addToRequests("memory", driverMemoryQuantity)
        .addToLimits("memory", driverMemoryLimitQuantity)
        .addToLimits(maybeCpuLimitQuantity.toMap.asJava)
        .endResources()
      .build()
    val baseDriverPod = new PodBuilder(driverSpec.driverPod)
      .editOrNewMetadata()
        .withName(kubernetesDriverPodName)
        .addToLabels(driverLabels.asJava)
        .addToAnnotations(allDriverAnnotations.asJava)
      .endMetadata()
      .withNewSpec()
        .withRestartPolicy("Never")
        .withNodeSelector(nodeSelector.asJava)
        .endSpec()
      .build()
    val resolvedSparkConf = driverSpec.driverSparkConf.clone()
      .setIfMissing(KUBERNETES_DRIVER_POD_NAME, kubernetesDriverPodName)
      .set("spark.app.id", kubernetesAppId)
      .set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, kubernetesResourceNamePrefix)
    driverSpec.copy(
      driverPod = baseDriverPod,
      driverSparkConf = resolvedSparkConf,
      driverContainer = driverContainer)
  }
}
