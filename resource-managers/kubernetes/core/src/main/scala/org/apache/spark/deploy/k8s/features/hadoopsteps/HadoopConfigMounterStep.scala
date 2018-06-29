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
package org.apache.spark.deploy.k8s.features.hadoopsteps

import java.io.File

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{EnvVarBuilder, KeyToPathBuilder, VolumeBuilder, VolumeMountBuilder}

import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging

 /**
  * This step is responsible for creating ConfigMaps containing Hadoop config files
  */
private[spark] class HadoopConfigMounterStep(
  hadoopConfConfigMapName: String,
  hadoopConfigFiles: Seq[File])
  extends HadoopConfigurationStep with Logging{

  override def configureHadoopSpec(hSpec: HadoopConfigSpec) : HadoopConfigSpec = {
    logInfo("HADOOP_CONF_DIR defined. Mounting Hadoop specific files")
    val keyPaths = hadoopConfigFiles.map { file =>
      val fileStringPath = file.toPath.getFileName.toString
      new KeyToPathBuilder()
          .withKey(fileStringPath)
          .withPath(fileStringPath)
        .build() }

    val hadoopFileVolume =
      new VolumeBuilder()
        .withName(HADOOP_FILE_VOLUME)
        .withNewConfigMap()
          .withName(hadoopConfConfigMapName)
          .withItems(keyPaths.asJava)
          .endConfigMap()
      .build()

    val hadoopEnvVals =
      new EnvVarBuilder()
        .withName(ENV_HADOOP_CONF_DIR)
        .withValue(HADOOP_CONF_DIR_PATH)
      .build()

    val hadoopVolumeMount =
      new VolumeMountBuilder()
      .withName(HADOOP_FILE_VOLUME)
      .withMountPath(HADOOP_CONF_DIR_PATH)
      .build()

     hSpec.copy(
       podVolumes = hSpec.podVolumes :+ hadoopFileVolume,
       containerEnvs = hSpec.containerEnvs :+ hadoopEnvVals,
       containerVMs = hSpec.containerVMs :+ hadoopVolumeMount,
       configMapProperties = hSpec.configMapProperties ++
         hadoopConfigFiles.map(file =>
           (file.toPath.getFileName.toString, Files.toString(file, Charsets.UTF_8))).toMap)
  }
}
