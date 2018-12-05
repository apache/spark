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
package org.apache.spark.deploy.k8s.features.hadooputils

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._

import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.SparkPod
import org.apache.spark.internal.Logging

private[spark] object HadoopBootstrapUtil extends Logging {

  /**
   * Mounting the DT secret for both the Driver and the executors
   *
   * @param dtSecretName Name of the secret that stores the Delegation Token
   * @param dtSecretItemKey Name of the Item Key storing the Delegation Token
   * @param userName Name of the SparkUser to set SPARK_USER
   * @param fileLocation Optional Location of the krb5 file
   * @param newKrb5ConfName Optional location of the ConfigMap for Krb5
   * @param existingKrb5ConfName Optional name of ConfigMap for Krb5
   * @param pod Input pod to be appended to
   * @return a modified SparkPod
   */
  def bootstrapKerberosPod(
      dtSecretName: String,
      dtSecretItemKey: String,
      userName: String,
      fileLocation: Option[String],
      newKrb5ConfName: Option[String],
      existingKrb5ConfName: Option[String],
      pod: SparkPod): SparkPod = {

    val preConfigMapVolume = existingKrb5ConfName.map { kconf =>
      new VolumeBuilder()
        .withName(KRB_FILE_VOLUME)
        .withNewConfigMap()
          .withName(kconf)
          .endConfigMap()
        .build()
    }

    val createConfigMapVolume = for {
      fLocation <- fileLocation
      krb5ConfName <- newKrb5ConfName
    } yield {
      val krb5File = new File(fLocation)
      val fileStringPath = krb5File.toPath.getFileName.toString
      new VolumeBuilder()
        .withName(KRB_FILE_VOLUME)
        .withNewConfigMap()
        .withName(krb5ConfName)
        .withItems(new KeyToPathBuilder()
          .withKey(fileStringPath)
          .withPath(fileStringPath)
          .build())
        .endConfigMap()
        .build()
    }

    // Breaking up Volume creation for clarity
    val configMapVolume = preConfigMapVolume.orElse(createConfigMapVolume)
    if (configMapVolume.isEmpty) {
       logInfo("You have not specified a krb5.conf file locally or via a ConfigMap. " +
         "Make sure that you have the krb5.conf locally on the Driver and Executor images")
    }

    val kerberizedPodWithDTSecret = new PodBuilder(pod.pod)
      .editOrNewSpec()
        .addNewVolume()
          .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
          .withNewSecret()
            .withSecretName(dtSecretName)
            .endSecret()
          .endVolume()
        .endSpec()
      .build()

    // Optionally add the krb5.conf ConfigMap
    val kerberizedPod = configMapVolume.map { cmVolume =>
      new PodBuilder(kerberizedPodWithDTSecret)
        .editSpec()
          .addNewVolumeLike(cmVolume)
            .endVolume()
          .endSpec()
        .build()
    }.getOrElse(kerberizedPodWithDTSecret)

    val kerberizedContainerWithMounts = new ContainerBuilder(pod.container)
      .addNewVolumeMount()
        .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
        .withMountPath(SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
        .endVolumeMount()
      .addNewEnv()
        .withName(ENV_HADOOP_TOKEN_FILE_LOCATION)
        .withValue(s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$dtSecretItemKey")
        .endEnv()
      .addNewEnv()
        .withName(ENV_SPARK_USER)
        .withValue(userName)
        .endEnv()
      .build()

    // Optionally add the krb5.conf Volume Mount
    val kerberizedContainer =
      if (configMapVolume.isDefined) {
        new ContainerBuilder(kerberizedContainerWithMounts)
          .addNewVolumeMount()
            .withName(KRB_FILE_VOLUME)
            .withMountPath(KRB_FILE_DIR_PATH + "/krb5.conf")
            .withSubPath("krb5.conf")
            .endVolumeMount()
          .build()
      } else {
        kerberizedContainerWithMounts
      }

    SparkPod(kerberizedPod, kerberizedContainer)
  }

  /**
   * setting ENV_SPARK_USER when HADOOP_FILES are detected
   *
   * @param sparkUserName Name of the SPARK_USER
   * @param pod Input pod to be appended to
   * @return a modified SparkPod
   */
  def bootstrapSparkUserPod(sparkUserName: String, pod: SparkPod): SparkPod = {
    val envModifiedContainer = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName(ENV_SPARK_USER)
        .withValue(sparkUserName)
        .endEnv()
      .build()
    SparkPod(pod.pod, envModifiedContainer)
  }

  /**
   * Grabbing files in the HADOOP_CONF_DIR
   *
   * @param path location of HADOOP_CONF_DIR
   * @return a list of File object
   */
  def getHadoopConfFiles(path: String): Seq[File] = {
    val dir = new File(path)
    if (dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toSeq
    } else {
      Seq.empty[File]
    }
  }

  /**
   * Bootstraping the container with ConfigMaps that store
   * Hadoop configuration files
   *
   * @param hadoopConfDir directory location of HADOOP_CONF_DIR env
   * @param newHadoopConfigMapName name of the new configMap for HADOOP_CONF_DIR
   * @param existingHadoopConfigMapName name of the pre-defined configMap for HADOOP_CONF_DIR
   * @param pod Input pod to be appended to
   * @return a modified SparkPod
   */
  def bootstrapHadoopConfDir(
      hadoopConfDir: Option[String],
      newHadoopConfigMapName: Option[String],
      existingHadoopConfigMapName: Option[String],
      pod: SparkPod): SparkPod = {
    val preConfigMapVolume = existingHadoopConfigMapName.map { hConf =>
      new VolumeBuilder()
        .withName(HADOOP_FILE_VOLUME)
        .withNewConfigMap()
          .withName(hConf)
          .endConfigMap()
        .build() }

    val createConfigMapVolume = for {
      dirLocation <- hadoopConfDir
      hConfName <- newHadoopConfigMapName
    } yield {
      val hadoopConfigFiles = getHadoopConfFiles(dirLocation)
      val keyPaths = hadoopConfigFiles.map { file =>
        val fileStringPath = file.toPath.getFileName.toString
        new KeyToPathBuilder()
          .withKey(fileStringPath)
          .withPath(fileStringPath)
          .build()
      }
      new VolumeBuilder()
        .withName(HADOOP_FILE_VOLUME)
        .withNewConfigMap()
          .withName(hConfName)
          .withItems(keyPaths.asJava)
          .endConfigMap()
        .build()
    }

    // Breaking up Volume Creation for clarity
    val configMapVolume = preConfigMapVolume.getOrElse(createConfigMapVolume.get)

    val hadoopSupportedPod = new PodBuilder(pod.pod)
      .editSpec()
        .addNewVolumeLike(configMapVolume)
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

  /**
   * Builds ConfigMap given the file location of the
   * krb5.conf file
   *
   * @param configMapName name of configMap for krb5
   * @param fileLocation location of krb5 file
   * @return a ConfigMap
   */
  def buildkrb5ConfigMap(
      configMapName: String,
      fileLocation: String): ConfigMap = {
    val file = new File(fileLocation)
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .endMetadata()
      .addToData(Map(file.toPath.getFileName.toString ->
        Files.toString(file, StandardCharsets.UTF_8)).asJava)
      .build()
  }

  /**
   * Builds ConfigMap given the ConfigMap name
   * and a list of Hadoop Conf files
   *
   * @param hadoopConfigMapName name of hadoopConfigMap
   * @param hadoopConfFiles list of hadoopFiles
   * @return a ConfigMap
   */
  def buildHadoopConfigMap(
      hadoopConfigMapName: String,
      hadoopConfFiles: Seq[File]): ConfigMap = {
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(hadoopConfigMapName)
        .endMetadata()
      .addToData(hadoopConfFiles.map { file =>
        (file.toPath.getFileName.toString,
          Files.toString(file, StandardCharsets.UTF_8))
        }.toMap.asJava)
      .build()
  }

}
