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

/**
 * Mounts the Hadoop configuration - either a pre-defined config map, or a local configuration
 * directory - on the driver pod.
 */
private[spark] class HadoopConfDriverFeatureStep(conf: KubernetesConf[_])
  extends KubernetesFeatureConfigStep {

  private val confSpec = conf.hadoopConfSpec
  private val confDir = confSpec.flatMap(_.hadoopConfDir)
  private val confMap = confSpec.flatMap(_.hadoopConfigMapName)

  private lazy val confFiles: Seq[File] = {
    val dir = new File(confDir.get)
    if (dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toSeq
    } else {
      Nil
    }
  }

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if confSpec.isDefined =>
      val confVolume = if (confDir.isDefined) {
        val keyPaths = confFiles.map { file =>
          new KeyToPathBuilder()
            .withKey(file.getName())
            .withPath(file.getName())
            .build()
        }
        new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
            .withName(conf.hadoopConfigMapName)
            .withItems(keyPaths.asJava)
            .endConfigMap()
          .build()
      } else {
        new VolumeBuilder()
          .withName(HADOOP_CONF_VOLUME)
          .withNewConfigMap()
            .withName(confMap.get)
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
      val fileMap = confFiles.map { file =>
        (file.getName(), Files.toString(file, StandardCharsets.UTF_8))
      }.toMap.asJava

      Seq(new ConfigMapBuilder()
        .withNewMetadata()
          .withName(conf.hadoopConfigMapName)
          .endMetadata()
        .addToData(fileMap)
        .build())
    } else {
      Nil
    }
  }

}
