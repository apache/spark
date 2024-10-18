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

import scala.jdk.CollectionConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.ArrayImplicits._

/**
 * Mounts the Hadoop configuration - either a pre-defined config map, or a local configuration
 * directory - on the driver pod.
 */
private[spark] class HadoopConfDriverFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  private val confDir = Option(conf.sparkConf.getenv(ENV_HADOOP_CONF_DIR))
  private val existingConfMap = conf.get(KUBERNETES_HADOOP_CONF_CONFIG_MAP)

  private lazy val confFiles: Seq[File] = {
    val dir = new File(confDir.get)
    if (dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toImmutableArraySeq
    } else {
      Nil
    }
  }

  private def newConfigMapName: String = s"${conf.resourceNamePrefix}-hadoop-config"

  private def hasHadoopConf: Boolean = confDir.isDefined || existingConfMap.isDefined

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if hasHadoopConf =>
      val confVolume = if (existingConfMap.isDefined) {
        new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
          .withName(existingConfMap.get)
          .endConfigMap()
          .build()
      } else {
        val keyPaths = confFiles.map { file =>
          new KeyToPathBuilder()
            .withKey(file.getName())
            .withPath(file.getName())
            .build()
        }
        new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
          .withName(newConfigMapName)
          .withItems(keyPaths.asJava)
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

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    if (hasHadoopConf) {
      Map(HADOOP_CONFIG_MAP_NAME -> existingConfMap.getOrElse(newConfigMapName))
    } else {
      Map.empty
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (confDir.isDefined && existingConfMap.isEmpty) {
      val fileMap = confFiles.map { file =>
        (file.getName(), Files.asCharSource(file, StandardCharsets.UTF_8).read())
      }.toMap.asJava

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
