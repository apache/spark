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

import java.lang.Long.parseLong

import org.apache.spark.SparkConf
import org.apache.spark.annotation.{DeveloperApi, Since, Unstable}
import org.apache.spark.deploy.k8s.Config._

/**
 * :: DeveloperApi ::
 *
 * A utility class used for K8s operations internally and Spark K8s operator.
 */
@Unstable
@DeveloperApi
object KubernetesVolumeUtils {
  /**
   * Extract Spark volume configuration properties with a given name prefix.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing with volume name as key and spec as value
   */
  @Since("3.0.0")
  def parseVolumesWithPrefix(sparkConf: SparkConf, prefix: String): Seq[KubernetesVolumeSpec] = {
    val properties = sparkConf.getAllWithPrefix(prefix).toMap

    getVolumeTypesAndNames(properties).map { case (volumeType, volumeName) =>
      val pathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_PATH_KEY"
      val readOnlyKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_READONLY_KEY"
      val subPathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY"
      val subPathExprKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_SUBPATHEXPR_KEY"
      val labelKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_LABEL_KEY"
      verifyMutuallyExclusiveOptionKeys(properties, subPathKey, subPathExprKey)

      val volumeLabelsMap = properties
        .filter(_._1.startsWith(labelKey))
        .map {
          case (k, v) => k.replaceAll(labelKey, "") -> v
        }

      KubernetesVolumeSpec(
        volumeName = volumeName,
        mountPath = properties(pathKey),
        mountSubPath = properties.getOrElse(subPathKey, ""),
        mountSubPathExpr = properties.getOrElse(subPathExprKey, ""),
        mountReadOnly = properties.get(readOnlyKey).exists(_.toBoolean),
        volumeConf = parseVolumeSpecificConf(properties,
          volumeType, volumeName, Option(volumeLabelsMap)))
    }.toSeq
  }

  /**
   * Get unique pairs of volumeType and volumeName,
   * assuming options are formatted in this way:
   * `volumeType`.`volumeName`.`property` = `value`
   * @param properties flat mapping of property names to values
   * @return Set[(volumeType, volumeName)]
   */
  private def getVolumeTypesAndNames(properties: Map[String, String]): Set[(String, String)] = {
    properties.keys.flatMap { k =>
      k.split('.').toList match {
        case tpe :: name :: _ => Some((tpe, name))
        case _ => None
      }
    }.toSet
  }

  private def parseVolumeSpecificConf(
      options: Map[String, String],
      volumeType: String,
      volumeName: String,
      labels: Option[Map[String, String]]): KubernetesVolumeSpecificConf = {
    volumeType match {
      case KUBERNETES_VOLUMES_HOSTPATH_TYPE =>
        val pathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_PATH_KEY"
        val typeKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_TYPE_KEY"
        verifyOptionKey(options, pathKey, KUBERNETES_VOLUMES_HOSTPATH_TYPE)
        // "" means that no checks will be performed before mounting the hostPath volume
        // backward compatibility default
        KubernetesHostPathVolumeConf(options(pathKey), options.getOrElse(typeKey, ""))

      case KUBERNETES_VOLUMES_PVC_TYPE =>
        val claimNameKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY"
        val storageClassKey =
          s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_CLAIM_STORAGE_CLASS_KEY"
        val sizeLimitKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY"
        verifyOptionKey(options, claimNameKey, KUBERNETES_VOLUMES_PVC_TYPE)
        verifySize(options.get(sizeLimitKey))
        KubernetesPVCVolumeConf(
          options(claimNameKey),
          options.get(storageClassKey),
          options.get(sizeLimitKey),
          labels)

      case KUBERNETES_VOLUMES_EMPTYDIR_TYPE =>
        val mediumKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY"
        val sizeLimitKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY"
        verifySize(options.get(sizeLimitKey))
        KubernetesEmptyDirVolumeConf(options.get(mediumKey), options.get(sizeLimitKey))

      case KUBERNETES_VOLUMES_NFS_TYPE =>
        val pathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_PATH_KEY"
        val serverKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_SERVER_KEY"
        verifyOptionKey(options, pathKey, KUBERNETES_VOLUMES_NFS_TYPE)
        verifyOptionKey(options, serverKey, KUBERNETES_VOLUMES_NFS_TYPE)
        KubernetesNFSVolumeConf(
          options(pathKey),
          options(serverKey))

      case _ =>
        throw new IllegalArgumentException(s"Kubernetes Volume type `$volumeType` is not supported")
    }
  }

  private def verifyOptionKey(options: Map[String, String], key: String, msg: String): Unit = {
    if (!options.isDefinedAt(key)) {
      throw new NoSuchElementException(key + s" is required for $msg")
    }
  }

  private def verifyMutuallyExclusiveOptionKeys(
      options: Map[String, String],
      keys: String*): Unit = {
    val givenKeys = keys.filter(options.contains)
    if (givenKeys.length > 1) {
      throw new IllegalArgumentException("These config options are mutually exclusive: " +
        s"${givenKeys.mkString(", ")}")
    }
  }

  private def verifySize(size: Option[String]): Unit = {
    size.foreach { v =>
      if (v.forall(_.isDigit) && parseLong(v) < 1024) {
        throw new IllegalArgumentException(
          s"Volume size `$v` is smaller than 1KiB. Missing units?")
      }
    }
  }
}
