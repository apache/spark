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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, KeyToPathBuilder, PodBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesRoleSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging

 /**
  * This step is responsible for bootstraping the container with ConfigMaps
  * containing Hadoop config files mounted as volumes and an ENV variable
  * pointed to the mounted file directory. This is run by both the driver
  * and executor, as they both require Hadoop config files.
  */
private[spark] class HadoopConfFeatureStep(
  kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep with Logging{

  override def configurePod(pod: SparkPod): SparkPod = {
    require(kubernetesConf.hadoopConfDir.isDefined, "Ensure that HADOOP_CONF_DIR is defined")
    logInfo("HADOOP_CONF_DIR defined. Mounting Hadoop specific files")
    val kubeTokenManager = kubernetesConf.getTokenManager
    val hadoopConfigFiles =
      kubeTokenManager.getHadoopConfFiles(kubernetesConf.hadoopConfDir.get)
    val keyPaths = hadoopConfigFiles.map { file =>
      val fileStringPath = file.toPath.getFileName.toString
      new KeyToPathBuilder()
        .withKey(fileStringPath)
        .withPath(fileStringPath)
        .build() }

    val hadoopSupportedPod = new PodBuilder(pod.pod)
      .editSpec()
        .addNewVolume()
          .withName(HADOOP_FILE_VOLUME)
          .withNewConfigMap()
            .withName(kubernetesConf.getHadoopConfigMapName)
            .withItems(keyPaths.asJava)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()

    val hadoopSupportedContainer = new ContainerBuilder(pod.container)
      .addNewVolumeMount()
        .withName(HADOOP_FILE_VOLUME)
        .withMountPath(HADOOP_CONF_DIR_PATH)
        .endVolumeMount()
      .addNewEnv()
        .withName(ENV_HADOOP_CONF_DIR)
        .withValue(HADOOP_CONF_DIR_PATH)
        .endEnv()
      .build()

    SparkPod(hadoopSupportedPod, hadoopSupportedContainer)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
