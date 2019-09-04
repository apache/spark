/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, KeyToPathBuilder, PodBuilder, VolumeBuilder}

import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._

/**
 * Mounts the Hadoop configuration for executor pods iff the driver pod is mounted.
 */
class HadoopConfExecutorFeatureStep(conf: KubernetesExecutorConf)
  extends KubernetesFeatureConfigStep {

  private val confFiles: Seq[File] = Option(conf.sparkConf.getenv(ENV_HADOOP_CONF_DIR)) match {
    case Some(dir) =>
      val file = new File(dir)
      if (file.isDirectory) file.listFiles().filter(_.isFile).toSeq else Nil
    case _ => Nil
  }

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if confFiles.nonEmpty =>
      val keyPaths = confFiles.map { file =>
        new KeyToPathBuilder().withKey(file.getName).withPath(file.getName).build()
      }.asJava

      val confVolume = new VolumeBuilder()
        .withName(HADOOP_CONF_VOLUME)
        .withNewConfigMap()
          .withName(conf.resourceNamePrefix + "-hadoop-config")
          .withItems(keyPaths)
          .endConfigMap()
        .build()

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
}
