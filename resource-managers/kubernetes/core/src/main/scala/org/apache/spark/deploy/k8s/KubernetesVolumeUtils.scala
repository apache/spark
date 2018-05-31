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

import scala.collection.mutable.HashMap

import io.fabric8.kubernetes.api.model._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._

private[spark] object KubernetesVolumeUtils {

  /**
   * Given hostPath volume specs, add volume to pod and volume mount to container.
   *
   * @param pod original specification of the pod
   * @param container original specification of the container
   * @param sparkConf Spark configuration
   * @param prefix the prefix for volume configuration
   * @return a tuple of (pod with the volume(s) added, container with mount(s) added)
   */
  def addVolumes(
      pod: Pod,
      container: Container,
      sparkConf: SparkConf,
      prefix : String): (Pod, Container) = {
    val hostPathVolumeSpecs = parseHostPathVolumesWithPrefix(sparkConf, prefix)
    addHostPathVolumes(pod, container, hostPathVolumeSpecs)
  }

  /**
   * Extract Spark volume configuration properties with a given name prefix.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @param volumeTypeKey the given property name prefix
   * @return a Map storing with volume name as key and spec as value
   */
  def parseVolumesWithPrefix(
      sparkConf: SparkConf,
      prefix: String,
      volumeTypeKey: String): Map[String, KubernetesVolumeSpec] = {
    val volumes = HashMap[String, KubernetesVolumeSpec]()
    val properties = sparkConf.getAllWithPrefix(s"$prefix$volumeTypeKey.").toList
    // Extract volume names
    properties.foreach {
      k =>
        val keys = k._1.split("\\.")
        if (keys.nonEmpty && !volumes.contains(keys(0))) {
          volumes.update(keys(0), KubernetesVolumeSpec.emptySpec())
        }
    }
    // Populate spec
    volumes.foreach {
      case (name, spec) =>
        properties.foreach {
          k =>
            k._1.split("\\.") match {
              case Array(`name`, KUBERNETES_VOLUMES_MOUNT_KEY, KUBERNETES_VOLUMES_PATH_KEY) =>
                spec.mountPath = Some(k._2)
              case Array(`name`, KUBERNETES_VOLUMES_MOUNT_KEY, KUBERNETES_VOLUMES_READONLY_KEY) =>
                spec.mountReadOnly = Some(k._2.toBoolean)
              case Array(`name`, KUBERNETES_VOLUMES_OPTIONS_KEY, option) =>
                spec.optionsSpec.update(option, k._2)
              case _ =>
                None
            }
        }
    }
    volumes.toMap
  }

  /**
   * Extract Spark hostPath volume configuration properties with a given name prefix and
   * return the result as a Map.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing with volume name as key and spec as value
   */
  def parseHostPathVolumesWithPrefix(
      sparkConf: SparkConf,
      prefix: String): Map[String, KubernetesVolumeSpec] = {
    parseVolumesWithPrefix(sparkConf, prefix, KUBERNETES_VOLUMES_HOSTPATH_KEY)
  }

  /**
   * Given hostPath volume specs, add volume to pod and volume mount to container.
   *
   * @param pod original specification of the pod
   * @param container original specification of the container
   * @param volumes list of named volume specs
   * @return a tuple of (pod with the volume(s) added, container with mount(s) added)
   */
  def addHostPathVolumes(
      pod: Pod,
      container: Container,
      volumes: Map[String, KubernetesVolumeSpec]): (Pod, Container) = {
    val podBuilder = new PodBuilder(pod).editOrNewSpec()
    val containerBuilder = new ContainerBuilder(container)
    volumes foreach {
      case (name, spec) =>
        var hostPath: Option[String] = None
        if (spec.optionsSpec.contains(KUBERNETES_VOLUMES_PATH_KEY)) {
          hostPath = Some(spec.optionsSpec(KUBERNETES_VOLUMES_PATH_KEY))
        }
        if (hostPath.isDefined && spec.mountPath.isDefined) {
          podBuilder.addToVolumes(new VolumeBuilder()
            .withHostPath(new HostPathVolumeSource(hostPath.get))
            .withName(name)
            .build())
          val volumeBuilder = new VolumeMountBuilder()
            .withMountPath(spec.mountPath.get)
            .withName(name)
          if (spec.mountReadOnly.isDefined) {
            containerBuilder
              .addToVolumeMounts(volumeBuilder
                .withReadOnly(spec.mountReadOnly.get)
                .build())
          } else {
            containerBuilder.addToVolumeMounts(volumeBuilder.build())
          }
        }
    }
    (podBuilder.endSpec().build(), containerBuilder.build())
  }
}
