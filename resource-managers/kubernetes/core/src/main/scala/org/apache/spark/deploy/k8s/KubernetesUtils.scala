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
import org.apache.spark.util.Utils

private[spark] object KubernetesUtils {

  /**
   * Extract and parse Spark configuration properties with a given name prefix and
   * return the result as a Map. Keys must not have more than one value.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing the configuration property keys and values
   */
  def parsePrefixedKeyValuePairs(
      sparkConf: SparkConf,
      prefix: String): Map[String, String] = {
    sparkConf.getAllWithPrefix(prefix).toMap
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
    val volumes = HashMap[String, KubernetesVolumeSpec]()
    val properties = sparkConf.getAllWithPrefix(s"$prefix$KUBERNETES_VOLUMES_HOSTPATH_KEY")
    // Extract volume names
    properties foreach {
      case (k, _) =>
        val keys = k.split(".")
        if (keys.nonEmpty && !volumes.contains(keys(0))) {
          volumes.update(keys(0), KubernetesVolumeSpec.emptySpec())
        }
    }
    // Populate spec
    volumes foreach {
      case (name, spec) =>
        properties foreach {
          case (k, v) =>
            k.split(".") match {
              case Array(`name`, KUBERNETES_VOLUMES_MOUNT_KEY, KUBERNETES_VOLUMES_PATH_KEY) =>
                spec.mountPath = Some(v)
              case Array(`name`, KUBERNETES_VOLUMES_MOUNT_KEY, KUBERNETES_VOLUMES_READONLY_KEY) =>
                spec.mountReadOnly = Some(v.toBoolean)
              case Array(`name`, KUBERNETES_VOLUMES_OPTIONS_KEY, option) =>
                spec.optionsSpec.update(option, v)
              case _ =>
                None
            }
        }
    }
    volumes.toMap
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

  def requireNandDefined(opt1: Option[_], opt2: Option[_], errMessage: String): Unit = {
    opt1.foreach { _ => require(opt2.isEmpty, errMessage) }
  }

  /**
   * For the given collection of file URIs, resolves them as follows:
   * - File URIs with scheme local:// resolve to just the path of the URI.
   * - Otherwise, the URIs are returned as-is.
   */
  def resolveFileUrisAndPath(fileUris: Iterable[String]): Iterable[String] = {
    fileUris.map { uri =>
      resolveFileUri(uri)
    }
  }

  private def resolveFileUri(uri: String): String = {
    val fileUri = Utils.resolveURI(uri)
    val fileScheme = Option(fileUri.getScheme).getOrElse("file")
    fileScheme match {
      case "local" => fileUri.getPath
      case _ => uri
    }
  }
}
