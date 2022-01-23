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
import java.nio.charset.{MalformedInputException, StandardCharsets}

import scala.collection.JavaConverters._
import com.google.common.io.Files

import io.fabric8.kubernetes.api.model._
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

import scala.collection.mutable
import scala.io.{Codec, Source}

/**
 * Mounts the Hadoop configuration - either a pre-defined config map, or a local configuration
 * directory - on the driver pod.
 */
private[spark] class HadoopConfDriverFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  private val confDir = Option(conf.sparkConf.getenv(ENV_HADOOP_CONF_DIR))
  private val existingConfMap = conf.get(KUBERNETES_HADOOP_CONF_CONFIG_MAP)

  KubernetesUtils.requireNandDefined(
    confDir,
    existingConfMap,
    "Do not specify both the `HADOOP_CONF_DIR` in your ENV and the ConfigMap " +
    "as the creation of an additional ConfigMap, when one is already specified is extraneous")

  private lazy val confFiles: Seq[File] = {
    val dir = new File(confDir.get)
    if (dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toSeq
    } else {
      Nil
    }
  }

  private def newConfigMapName: String = s"${conf.resourceNamePrefix}-hadoop-config"

  private def hasHadoopConf: Boolean = confDir.isDefined || existingConfMap.isDefined

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if hasHadoopConf =>
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
            .withName(newConfigMapName)
            .withItems(keyPaths.asJava)
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
      val fileMap = loadSparkConfDirFiles(confFiles).asJava

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

  private def loadSparkConfDirFiles(confFiles: Seq[File]): Map[String, String] = {
    val maxSize = conf.get(Config.CONFIG_MAP_MAXSIZE)
    var truncatedMapSize: Long = 0
    val truncatedMap = mutable.HashMap[String, String]()
    val skippedFiles = mutable.HashSet[String]()
    var source: Source = Source.fromString("") // init with empty source.
    for (file <- confFiles) {
      try {
        source = Source.fromFile(file)(Codec.UTF8)
        val (fileName, fileContent) = file.getName -> source.mkString
        if ((truncatedMapSize + fileName.length + fileContent.length) < maxSize) {
          truncatedMap.put(fileName, fileContent)
          truncatedMapSize = truncatedMapSize + (fileName.length + fileContent.length)
        } else {
          skippedFiles.add(fileName)
        }
      } catch {
        case e: MalformedInputException =>
          logWarning(
            s"Unable to read a non UTF-8 encoded file ${file.getAbsolutePath}. Skipping...", e)
          None
      } finally {
        source.close()
      }
    }
    if (truncatedMap.nonEmpty) {
      logInfo(s"Hadoop configuration files loaded from $confDir :" +
        s" ${truncatedMap.keys.mkString(",")}")
    }
    if (skippedFiles.nonEmpty) {
      logWarning(s"Skipped hadoop conf file(s) ${skippedFiles.mkString(",")}, due to size constraint." +
        s" Please see, config: `${Config.CONFIG_MAP_MAXSIZE.key}` for more details.")
    }
    truncatedMap.toMap
  }
}
