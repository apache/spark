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
import java.util.UUID

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.SparkPod
import org.apache.spark.deploy.k8s.features.KubernetesFeaturesTestUtils._
import org.apache.spark.util.Utils

class HadoopBootstrapUtilSuite extends SparkFunSuite with BeforeAndAfter{
  private val sparkPod = SparkPod.initialPod()
  private val hadoopBootstrapUtil = new HadoopBootstrapUtil
  private var tmpDir: File = _
  private var tmpFile: File = _

  before {
    tmpDir = Utils.createTempDir()
    tmpFile = File.createTempFile(s"${UUID.randomUUID().toString}", ".txt", tmpDir)
    Files.write("contents".getBytes, tmpFile)
  }

  after {
    tmpFile.delete()
    tmpDir.delete()
  }

  test("bootstrapKerberosPod with file location specified for krb5.conf file") {
    val dtSecretName = "EXAMPLE_SECRET_NAME"
    val dtSecretItemKey = "EXAMPLE_ITEM_KEY"
    val userName = "SPARK_USER_NAME"
    val fileLocation = Some(tmpFile.getAbsolutePath)
    val stringPath = tmpFile.getName
    val newKrb5ConfName = Some("/etc/krb5.conf")
    val resultingPod = hadoopBootstrapUtil.bootstrapKerberosPod(
      dtSecretName,
      dtSecretItemKey,
      userName,
      fileLocation,
      newKrb5ConfName,
      None,
      sparkPod)
    val expectedVolumes = Seq(
      new VolumeBuilder()
        .withName(KRB_FILE_VOLUME)
        .withNewConfigMap()
          .withName(newKrb5ConfName.get)
          .withItems(new KeyToPathBuilder()
            .withKey(stringPath)
            .withPath(stringPath)
            .build())
        .endConfigMap()
        .build(),
      new VolumeBuilder()
        .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
        .withNewSecret()
          .withSecretName(dtSecretName)
          .endSecret()
        .build()
    )
    podHasVolumes(resultingPod.pod, expectedVolumes)
    containerHasEnvVars(resultingPod.container, Map(
      ENV_HADOOP_TOKEN_FILE_LOCATION -> s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$dtSecretItemKey",
      ENV_SPARK_USER -> userName)
    )
    containerHasVolumeMounts(resultingPod.container, Map(
      SPARK_APP_HADOOP_SECRET_VOLUME_NAME -> SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR,
      KRB_FILE_VOLUME -> (KRB_FILE_DIR_PATH + "/krb5.conf"))
    )
  }

  test("bootstrapKerberosPod with pre-existing configMap specified for krb5.conf file") {
    val dtSecretName = "EXAMPLE_SECRET_NAME"
    val dtSecretItemKey = "EXAMPLE_ITEM_KEY"
    val userName = "SPARK_USER_NAME"
    val existingKrb5ConfName = Some("krb5CMap")
    val resultingPod = hadoopBootstrapUtil.bootstrapKerberosPod(
      dtSecretName,
      dtSecretItemKey,
      userName,
      None,
      None,
      existingKrb5ConfName,
      sparkPod)
    val expectedVolumes = Seq(
      new VolumeBuilder()
        .withName(KRB_FILE_VOLUME)
        .withNewConfigMap()
          .withName(existingKrb5ConfName.get)
          .endConfigMap()
        .build(),
      new VolumeBuilder()
        .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
        .withNewSecret()
          .withSecretName(dtSecretName)
          .endSecret()
        .build()
    )
    podHasVolumes(resultingPod.pod, expectedVolumes)
    containerHasEnvVars(resultingPod.container, Map(
      ENV_HADOOP_TOKEN_FILE_LOCATION -> s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$dtSecretItemKey",
      ENV_SPARK_USER -> userName)
    )
    containerHasVolumeMounts(resultingPod.container, Map(
      SPARK_APP_HADOOP_SECRET_VOLUME_NAME -> SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
    )
  }

  test("default bootstrapSparkUserPod") {
    val userName = "SPARK_USER_NAME"
    val resultingPod = hadoopBootstrapUtil.bootstrapSparkUserPod(userName, sparkPod)
    containerHasEnvVars(resultingPod.container, Map(ENV_SPARK_USER -> userName))
  }

  test("bootstrapHadoopConfDir with directory location specified for HADOOP_CONF") {
    val hadoopConfDir = Some(tmpDir.getAbsolutePath)
    val stringPath = tmpFile.getName
    val newHadoopConfigMapName = Some("hconfMapName")
    val resultingPod = hadoopBootstrapUtil.bootstrapHadoopConfDir(
      hadoopConfDir,
      newHadoopConfigMapName,
      None,
      sparkPod
    )
    containerHasVolumeMounts(resultingPod.container, Map(
      HADOOP_FILE_VOLUME -> HADOOP_CONF_DIR_PATH)
    )
    val expectedVolumes = Seq(
      new VolumeBuilder()
        .withName(HADOOP_FILE_VOLUME)
        .withNewConfigMap()
          .withName(newHadoopConfigMapName.get)
          .withItems(new KeyToPathBuilder()
            .withKey(stringPath)
            .withPath(stringPath)
            .build())
          .endConfigMap()
        .build()
    )

    podHasVolumes(resultingPod.pod, expectedVolumes)
    containerHasVolumeMounts(resultingPod.container, Map(
      HADOOP_FILE_VOLUME -> HADOOP_CONF_DIR_PATH)
    )
    containerHasEnvVars(resultingPod.container, Map(
      ENV_HADOOP_CONF_DIR -> HADOOP_CONF_DIR_PATH)
    )
  }

  test("bootstrapHadoopConfDir with pre-existing configMap, storing HADOOP_CONF files, specified") {
    val existingHadoopConfigMapName = Some("hconfMapName")
    val resultingPod = hadoopBootstrapUtil.bootstrapHadoopConfDir(
      None,
      None,
      existingHadoopConfigMapName,
      sparkPod
    )
    containerHasVolumeMounts(resultingPod.container, Map(
      HADOOP_FILE_VOLUME -> HADOOP_CONF_DIR_PATH)
    )
    val expectedVolumes = Seq(
      new VolumeBuilder()
        .withName(HADOOP_FILE_VOLUME)
          .withNewConfigMap()
            .withName(existingHadoopConfigMapName.get)
            .endConfigMap()
        .build())
    podHasVolumes(resultingPod.pod, expectedVolumes)
  }

  test("default buildKrb5ConfigMap") {
    val configMapName = "hconfMapName"
    val resultingCMap = hadoopBootstrapUtil.buildkrb5ConfigMap(
      configMapName,
      tmpFile.getAbsolutePath
    )
    assertHelper(resultingCMap, new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .endMetadata()
      .addToData(Map(tmpFile.getName -> "contents").asJava)
    .build())
  }

  test("buildHadoopConfigMap on simple file") {
    val configMapName = "hconfMapName"
    val resultingCMap = hadoopBootstrapUtil.buildHadoopConfigMap(
      configMapName,
      Seq(tmpFile)
    )
    assertHelper(resultingCMap, new ConfigMapBuilder()
      .withNewMetadata()
      .withName(configMapName)
      .endMetadata()
      .addToData(Map(tmpFile.getName -> "contents").asJava)
      .build())
  }
}
