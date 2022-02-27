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
package org.apache.spark.deploy.k8s.features

import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils
import org.apache.spark.internal.Logging

/**
 * Mounts the Hadoop configuration - either a pre-defined config map, or a local configuration
 * directory - on the driver pod.
 */
private[spark] class HadoopConfDriverFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep with Logging{

  private val confDir = Option(conf.sparkConf.getenv(ENV_HADOOP_CONF_DIR))
  private val existingConfMap = conf.get(KUBERNETES_HADOOP_CONF_CONFIG_MAP)

  KubernetesUtils.requireNandDefined(
    confDir,
    existingConfMap,
    "Do not specify both the `HADOOP_CONF_DIR` in your ENV and the ConfigMap " +
    "as the creation of an additional ConfigMap, when one is already specified is extraneous")

  private lazy val confFilesMap: Map[String, String] = {
    val maxSize = conf.get(Config.CONFIG_MAP_MAXSIZE)
    KubernetesClientUtils.loadHadoopConfDirFiles(confDir, maxSize)
  }

  private def newConfigMapName: String = s"${conf.resourceNamePrefix}-hadoop-config"

  private def hasHadoopConf: Boolean = confDir.isDefined || existingConfMap.isDefined

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if hasHadoopConf =>
      val confVolume = if (confDir.isDefined) {
        val keyPaths = confFilesMap.map {
          case (fileName: String, _:String) =>
            new KeyToPathBuilder()
              .withKey(fileName)
              .withPath(fileName)
              .build()
        }
        new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
            .withName(newConfigMapName)
            .withItems(keyPaths.toList.asJava)
            .endConfigMap()
          .build()
      } else {
        new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
            .withName(existingConfMap.get)
            .endConfigMap()
          .build()
      }

      val podWithConf = new PodBuilder(pod.pod)
        .editSpec()
          .addNewVolumeLike(confVolume)
            .endVolume()
          .endSpec()
          .build()

      val containerWithMount = new ContainerBuilder(pod.container)
        .addNewVolumeMount()
          .withName(HADOOP_CONF_VOLUME)
          .withMountPath(HADOOP_CONF_DIR_PATH)
          .endVolumeMount()
        .addNewEnv()
          .withName(ENV_HADOOP_CONF_DIR)
          .withValue(HADOOP_CONF_DIR_PATH)
          .endEnv()
        .build()

      SparkPod(podWithConf, containerWithMount)
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (confDir.isDefined) {
      val fileMap = confFilesMap.asJava

      Seq(new ConfigMapBuilder()
        .withNewMetadata()
          .withName(newConfigMapName)
          .endMetadata()
        .withImmutable(true)
        .addToData(fileMap)
        .build())
    } else {
      Nil
    }
  }

}
