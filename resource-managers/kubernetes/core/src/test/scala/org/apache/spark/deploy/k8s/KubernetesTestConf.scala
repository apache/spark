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

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.submit.{JavaMainAppResource, MainAppResource}
import org.apache.spark.util.{Clock, SystemClock}

/**
 * Builder methods for KubernetesConf that allow easy control over what to return for a few
 * properties. For use with tests instead of having to mock specific properties.
 */
object KubernetesTestConf {

  val APP_ID = "appId"
  val MAIN_CLASS = "mainClass"
  val RESOURCE_PREFIX = "prefix"
  val EXECUTOR_ID = "1"

  private val DEFAULT_CONF = new SparkConf(false)

  // scalastyle:off argcount
  def createDriverConf(
      sparkConf: SparkConf = DEFAULT_CONF,
      appId: String = APP_ID,
      mainAppResource: MainAppResource = JavaMainAppResource(None),
      mainClass: String = MAIN_CLASS,
      appArgs: Array[String] = Array.empty,
      resourceNamePrefix: Option[String] = None,
      labels: Map[String, String] = Map.empty,
      environment: Map[String, String] = Map.empty,
      annotations: Map[String, String] = Map.empty,
      serviceLabels: Map[String, String] = Map.empty,
      serviceAnnotations: Map[String, String] = Map.empty,
      secretEnvNamesToKeyRefs: Map[String, String] = Map.empty,
      secretNamesToMountPaths: Map[String, String] = Map.empty,
      volumes: Seq[KubernetesVolumeSpec] = Seq.empty,
      proxyUser: Option[String] = None,
      clock: Clock = new SystemClock()): KubernetesDriverConf = {
    val conf = sparkConf.clone()

    resourceNamePrefix.foreach { prefix =>
      conf.set(KUBERNETES_DRIVER_POD_NAME_PREFIX, prefix)
    }
    setPrefixedConfigs(conf, KUBERNETES_DRIVER_LABEL_PREFIX, labels)
    setPrefixedConfigs(conf, KUBERNETES_DRIVER_ENV_PREFIX, environment)
    setPrefixedConfigs(conf, KUBERNETES_DRIVER_ANNOTATION_PREFIX, annotations)
    setPrefixedConfigs(conf, KUBERNETES_DRIVER_SERVICE_LABEL_PREFIX, serviceLabels)
    setPrefixedConfigs(conf, KUBERNETES_DRIVER_SERVICE_ANNOTATION_PREFIX, serviceAnnotations)
    setPrefixedConfigs(conf, KUBERNETES_DRIVER_SECRETS_PREFIX, secretNamesToMountPaths)
    setPrefixedConfigs(conf, KUBERNETES_DRIVER_SECRET_KEY_REF_PREFIX, secretEnvNamesToKeyRefs)
    setVolumeSpecs(conf, KUBERNETES_DRIVER_VOLUMES_PREFIX, volumes)

    new KubernetesDriverConf(conf, appId, mainAppResource, mainClass, appArgs, proxyUser, clock)
  }
  // scalastyle:on argcount

  def createExecutorConf(
      sparkConf: SparkConf = DEFAULT_CONF,
      driverPod: Option[Pod] = None,
      labels: Map[String, String] = Map.empty,
      environment: Map[String, String] = Map.empty,
      annotations: Map[String, String] = Map.empty,
      secretEnvNamesToKeyRefs: Map[String, String] = Map.empty,
      secretNamesToMountPaths: Map[String, String] = Map.empty,
      volumes: Seq[KubernetesVolumeSpec] = Seq.empty): KubernetesExecutorConf = {
    val conf = sparkConf.clone()

    setPrefixedConfigs(conf, KUBERNETES_EXECUTOR_LABEL_PREFIX, labels)
    setPrefixedConfigs(conf, "spark.executorEnv.", environment)
    setPrefixedConfigs(conf, KUBERNETES_EXECUTOR_ANNOTATION_PREFIX, annotations)
    setPrefixedConfigs(conf, KUBERNETES_EXECUTOR_SECRETS_PREFIX, secretNamesToMountPaths)
    setPrefixedConfigs(conf, KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX, secretEnvNamesToKeyRefs)
    setVolumeSpecs(conf, KUBERNETES_EXECUTOR_VOLUMES_PREFIX, volumes)

    new KubernetesExecutorConf(conf, APP_ID, EXECUTOR_ID, driverPod)
  }

  private def setPrefixedConfigs(
      conf: SparkConf,
      prefix: String,
      values: Map[String, String]): Unit = {
    values.foreach { case (k, v) =>
      conf.set(s"${prefix}$k", v)
    }
  }

  private def setVolumeSpecs(
      conf: SparkConf,
      prefix: String,
      volumes: Seq[KubernetesVolumeSpec]): Unit = {
    def key(vtype: String, vname: String, subkey: String): String = {
      s"${prefix}$vtype.$vname.$subkey"
    }

    volumes.foreach { case spec =>
      val (vtype, configs) = spec.volumeConf match {
        case KubernetesHostPathVolumeConf(path) =>
          (KUBERNETES_VOLUMES_HOSTPATH_TYPE,
            Map(KUBERNETES_VOLUMES_OPTIONS_PATH_KEY -> path))

        case KubernetesPVCVolumeConf(claimName, storageClass, sizeLimit, labels) =>
          val sconf = storageClass
            .map { s => (KUBERNETES_VOLUMES_OPTIONS_CLAIM_STORAGE_CLASS_KEY, s) }.toMap
          val lconf = sizeLimit.map { l => (KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY, l) }.toMap
          (KUBERNETES_VOLUMES_PVC_TYPE,
            Map(KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY -> claimName) ++ sconf ++ lconf)

        case KubernetesEmptyDirVolumeConf(medium, sizeLimit) =>
          val mconf = medium.map { m => (KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY, m) }.toMap
          val lconf = sizeLimit.map { l => (KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY, l) }.toMap
          (KUBERNETES_VOLUMES_EMPTYDIR_TYPE, mconf ++ lconf)

        case KubernetesNFSVolumeConf(path, server) =>
          (KUBERNETES_VOLUMES_NFS_TYPE, Map(
            KUBERNETES_VOLUMES_OPTIONS_PATH_KEY -> path,
            KUBERNETES_VOLUMES_OPTIONS_SERVER_KEY -> server))
      }

      conf.set(key(vtype, spec.volumeName, KUBERNETES_VOLUMES_MOUNT_PATH_KEY), spec.mountPath)
      if (spec.mountSubPath.nonEmpty) {
        conf.set(key(vtype, spec.volumeName, KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY),
          spec.mountSubPath)
      }
      conf.set(key(vtype, spec.volumeName, KUBERNETES_VOLUMES_MOUNT_READONLY_KEY),
        spec.mountReadOnly.toString)
      configs.foreach { case (k, v) =>
        conf.set(key(vtype, spec.volumeName, k), v)
      }
    }
  }

}
