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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._

private[spark] object KubernetesVolumeUtils {
  /**
   * Extract Spark volume configuration properties with a given name prefix.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing with volume name as key and spec as value
   */
  def parseVolumesWithPrefix(sparkConf: SparkConf, prefix: String): Seq[KubernetesVolumeSpec] = {
    val properties = sparkConf.getAllWithPrefix(prefix).toMap

    getVolumeTypesAndNames(properties).map { case (volumeType, volumeName) =>
      val pathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_PATH_KEY"
      val readOnlyKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_READONLY_KEY"
      val subPathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY"

      KubernetesVolumeSpec(
        volumeName = volumeName,
        mountPath = properties(pathKey),
        mountSubPath = properties.get(subPathKey).getOrElse(""),
        mountReadOnly = properties.get(readOnlyKey).exists(_.toBoolean),
        volumeConf = parseVolumeSpecificConf(properties, volumeType, volumeName))
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
      volumeName: String): KubernetesVolumeSpecificConf = {
    volumeType match {
      case KUBERNETES_VOLUMES_HOSTPATH_TYPE =>
        val pathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_PATH_KEY"
        KubernetesHostPathVolumeConf(options(pathKey))

      case KUBERNETES_VOLUMES_PVC_TYPE =>
        val claimNameKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY"
        KubernetesPVCVolumeConf(options(claimNameKey))

      case KUBERNETES_VOLUMES_EMPTYDIR_TYPE =>
        val mediumKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY"
        val sizeLimitKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY"
        KubernetesEmptyDirVolumeConf(options.get(mediumKey), options.get(sizeLimitKey))

      case _ =>
        throw new IllegalArgumentException(s"Kubernetes Volume type `$volumeType` is not supported")
    }
  }
}
