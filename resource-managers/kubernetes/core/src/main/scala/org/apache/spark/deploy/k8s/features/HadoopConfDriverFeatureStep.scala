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

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging

/**
 * Mounts the Hadoop configuration - either a pre-defined config map, or a local configuration
 * directory - on the driver pod.
 */
private[spark] class HadoopConfDriverFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep with Logging {

  private val confDir = Option(conf.sparkConf.getenv(ENV_HADOOP_CONF_DIR))
  private val existingConfMap = conf.get(KUBERNETES_HADOOP_CONF_CONFIG_MAP)

  private lazy val confFiles: Seq[File] = {
    val dir = new File(confDir.get)
    if (dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toSeq
    } else {
      Nil
    }
  }

  private def newConfigMapName: String = s"${conf.resourceNamePrefix}-hadoop-config"

  private def newPodWithConf: (SparkPod, Volume) => SparkPod = { (pod, volume) =>
    val podWithConf = new PodBuilder(pod.pod)
      .editSpec()
        .addNewVolumeLike(volume)
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

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform {
      case pod if existingConfMap.isDefined =>
        val configMapName = existingConfMap.get
        logInfo(s"Property ${KUBERNETES_HADOOP_CONF_CONFIG_MAP.key} -> $configMapName is" +
          s" detected and will be mounted on the driver pod.")
        val confVolume = new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
            .withName(configMapName)
            .endConfigMap()
          .build()
        newPodWithConf(pod, confVolume)
      case pod if confDir.isDefined =>
        val keyPaths = confFiles.map { file =>
          logDebug(s"Local hadoop config file ${file.getAbsolutePath} will be mounted on driver" +
            s" pod in $HADOOP_CONF_DIR_PATH")
          new KeyToPathBuilder()
            .withKey(file.getName)
            .withPath(file.getName)
            .build()
        }
        val confVolume = new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
            .withName(newConfigMapName)
            .withItems(keyPaths.asJava)
            .endConfigMap()
          .build()
        newPodWithConf(pod, confVolume)
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (existingConfMap.isEmpty && confDir.isDefined) {
      val fileMap = confFiles.map { file =>
        (file.getName, Files.toString(file, StandardCharsets.UTF_8))
      }.toMap.asJava

      Seq(new ConfigMapBuilder()
        .withNewMetadata()
          .withName(newConfigMapName)
          .endMetadata()
        .addToData(fileMap)
        .build())
    } else {
      Nil
    }
  }
}
