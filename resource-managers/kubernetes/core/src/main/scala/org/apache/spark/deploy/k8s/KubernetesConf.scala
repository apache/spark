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
package org.apache.spark.deploy.k8s

import scala.collection.mutable

import io.fabric8.kubernetes.api.model.{LocalObjectReference, LocalObjectReferenceBuilder, Pod}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit._
import org.apache.spark.deploy.k8s.submit.KubernetesClientApplication._
import org.apache.spark.internal.config.ConfigEntry


private[spark] sealed trait KubernetesRoleSpecificConf

/*
 * Structure containing metadata for Kubernetes logic that builds a Spark driver.
 */
private[spark] case class KubernetesDriverSpecificConf(
    mainAppResource: Option[MainAppResource],
    mainClass: String,
    appName: String,
    appArgs: Seq[String]) extends KubernetesRoleSpecificConf

/*
 * Structure containing metadata for Kubernetes logic that builds a Spark executor.
 */
private[spark] case class KubernetesExecutorSpecificConf(
    executorId: String,
    driverPod: Option[Pod])
  extends KubernetesRoleSpecificConf

/**
 * Structure containing metadata for Kubernetes logic to build Spark pods.
 */
private[spark] case class KubernetesConf[T <: KubernetesRoleSpecificConf](
    sparkConf: SparkConf,
    roleSpecificConf: T,
    appResourceNamePrefix: String,
    appId: String,
    roleLabels: Map[String, String],
    roleAnnotations: Map[String, String],
    roleSecretNamesToMountPaths: Map[String, String],
    roleSecretEnvNamesToKeyRefs: Map[String, String],
    roleEnvs: Map[String, String],
    roleVolumes: Iterable[KubernetesVolumeSpec[_ <: KubernetesVolumeSpecificConf]],
    sparkFiles: Seq[String]) {

  def namespace(): String = sparkConf.get(KUBERNETES_NAMESPACE)

  def sparkJars(): Seq[String] = sparkConf
    .getOption("spark.jars")
    .map(str => str.split(",").toSeq)
    .getOrElse(Seq.empty[String])

  def pyFiles(): Option[String] = sparkConf
    .get(KUBERNETES_PYSPARK_PY_FILES)

  def pySparkMainResource(): Option[String] = sparkConf
    .get(KUBERNETES_PYSPARK_MAIN_APP_RESOURCE)

  def pySparkPythonVersion(): String = sparkConf
      .get(PYSPARK_MAJOR_PYTHON_VERSION)

  def sparkRMainResource(): Option[String] = sparkConf
    .get(KUBERNETES_R_MAIN_APP_RESOURCE)

  def imagePullPolicy(): String = sparkConf.get(CONTAINER_IMAGE_PULL_POLICY)

  def imagePullSecrets(): Seq[LocalObjectReference] = {
    sparkConf
      .get(IMAGE_PULL_SECRETS)
      .map(_.split(","))
      .getOrElse(Array.empty[String])
      .map(_.trim)
      .map { secret =>
        new LocalObjectReferenceBuilder().withName(secret).build()
      }
  }

  def nodeSelector(): Map[String, String] =
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_NODE_SELECTOR_PREFIX)

  def get[T](config: ConfigEntry[T]): T = sparkConf.get(config)

  def get(conf: String): String = sparkConf.get(conf)

  def get(conf: String, defaultValue: String): String = sparkConf.get(conf, defaultValue)

  def getOption(key: String): Option[String] = sparkConf.getOption(key)
}

private[spark] object KubernetesConf {
  def createDriverConf(
      sparkConf: SparkConf,
      appName: String,
      appResourceNamePrefix: String,
      appId: String,
      mainAppResource: Option[MainAppResource],
      mainClass: String,
      appArgs: Array[String],
      maybePyFiles: Option[String]): KubernetesConf[KubernetesDriverSpecificConf] = {
    val sparkConfWithMainAppJar = sparkConf.clone()
    val additionalFiles = mutable.ArrayBuffer.empty[String]
    mainAppResource.foreach {
        case JavaMainAppResource(res) =>
          val previousJars = sparkConf
            .getOption("spark.jars")
            .map(_.split(","))
            .getOrElse(Array.empty)
          if (!previousJars.contains(res)) {
            sparkConfWithMainAppJar.setJars(previousJars ++ Seq(res))
          }
        // The function of this outer match is to account for multiple nonJVM
        // bindings that will all have increased default MEMORY_OVERHEAD_FACTOR to 0.4
        case nonJVM: NonJVMResource =>
          nonJVM match {
            case PythonMainAppResource(res) =>
              additionalFiles += res
              maybePyFiles.foreach{maybePyFiles =>
                additionalFiles.appendAll(maybePyFiles.split(","))}
              sparkConfWithMainAppJar.set(KUBERNETES_PYSPARK_MAIN_APP_RESOURCE, res)
            case RMainAppResource(res) =>
              additionalFiles += res
              sparkConfWithMainAppJar.set(KUBERNETES_R_MAIN_APP_RESOURCE, res)
          }
          sparkConfWithMainAppJar.setIfMissing(MEMORY_OVERHEAD_FACTOR, 0.4)
    }

    val driverCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_LABEL_PREFIX)
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), "Label with key " +
      s"$SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    require(!driverCustomLabels.contains(SPARK_ROLE_LABEL), "Label with key " +
      s"$SPARK_ROLE_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    val driverLabels = driverCustomLabels ++ Map(
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
    val driverAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_ANNOTATION_PREFIX)
    val driverSecretNamesToMountPaths = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_SECRETS_PREFIX)
    val driverSecretEnvNamesToKeyRefs = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_SECRET_KEY_REF_PREFIX)
    val driverEnvs = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_ENV_PREFIX)
    val driverVolumes = KubernetesVolumeUtils.parseVolumesWithPrefix(
      sparkConf, KUBERNETES_DRIVER_VOLUMES_PREFIX).map(_.get)
    // Also parse executor volumes in order to verify configuration
    // before the driver pod is created
    KubernetesVolumeUtils.parseVolumesWithPrefix(
      sparkConf, KUBERNETES_EXECUTOR_VOLUMES_PREFIX).map(_.get)

    val sparkFiles = sparkConf
      .getOption("spark.files")
      .map(str => str.split(",").toSeq)
      .getOrElse(Seq.empty[String]) ++ additionalFiles

    KubernetesConf(
      sparkConfWithMainAppJar,
      KubernetesDriverSpecificConf(mainAppResource, mainClass, appName, appArgs),
      appResourceNamePrefix,
      appId,
      driverLabels,
      driverAnnotations,
      driverSecretNamesToMountPaths,
      driverSecretEnvNamesToKeyRefs,
      driverEnvs,
      driverVolumes,
      sparkFiles)
  }

  def createExecutorConf(
      sparkConf: SparkConf,
      executorId: String,
      appId: String,
      driverPod: Option[Pod]): KubernetesConf[KubernetesExecutorSpecificConf] = {
    val executorCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_LABEL_PREFIX)
    require(
      !executorCustomLabels.contains(SPARK_APP_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_APP_ID_LABEL as it is reserved for Spark.")
    require(
      !executorCustomLabels.contains(SPARK_EXECUTOR_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_EXECUTOR_ID_LABEL as it is reserved for" +
        " Spark.")
    require(
      !executorCustomLabels.contains(SPARK_ROLE_LABEL),
      s"Custom executor labels cannot contain $SPARK_ROLE_LABEL as it is reserved for Spark.")
    val executorLabels = Map(
      SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE) ++
      executorCustomLabels
    val executorAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_ANNOTATION_PREFIX)
    val executorMountSecrets = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_SECRETS_PREFIX)
    val executorEnvSecrets = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX)
    val executorEnv = sparkConf.getExecutorEnv.toMap
    val executorVolumes = KubernetesVolumeUtils.parseVolumesWithPrefix(
      sparkConf, KUBERNETES_EXECUTOR_VOLUMES_PREFIX).map(_.get)

    // If no prefix is defined then we are in pure client mode
    // (not the one used by cluster mode inside the container)
    val appResourceNamePrefix = {
      if (sparkConf.getOption(KUBERNETES_EXECUTOR_POD_NAME_PREFIX.key).isEmpty) {
        getResourceNamePrefix(getAppName(sparkConf))
      } else {
        sparkConf.get(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)
      }
    }

    KubernetesConf(
      sparkConf.clone(),
      KubernetesExecutorSpecificConf(executorId, driverPod),
      appResourceNamePrefix,
      appId,
      executorLabels,
      executorAnnotations,
      executorMountSecrets,
      executorEnvSecrets,
      executorEnv,
      executorVolumes,
      Seq.empty[String])
  }
}
