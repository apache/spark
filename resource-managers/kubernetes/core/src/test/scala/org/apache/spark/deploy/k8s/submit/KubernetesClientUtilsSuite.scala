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

package org.apache.spark.deploy.k8s.submit

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.{HashMap => JHashMap}
import java.util.UUID

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Config, Constants}
import org.apache.spark.util.Utils

class KubernetesClientUtilsSuite extends SparkFunSuite with BeforeAndAfter {

  def testSetup(inputFiles: Map[String, Array[Byte]]): SparkConf = {
    val tempDir = Utils.createTempDir()
    val sparkConf = new SparkConf(loadDefaults = false)
      .setSparkHome(tempDir.getAbsolutePath)

    val tempConfDir = new File(s"${tempDir.getAbsolutePath}/conf")
    tempConfDir.mkdir()
    for (i <- inputFiles) yield {
      val file = new File(s"${tempConfDir.getAbsolutePath}/${i._1}")
      Files.write(file.toPath, i._2)
      file.getName
    }
    sparkConf
  }

  test("verify load files, loads only allowed files and not the disallowed files.") {
    val input: Map[String, Array[Byte]] = Map("test.txt" -> "test123", "z12.zip" -> "zZ",
      "rere.jar" -> "@31", "spark.jar" -> "@31", "_test" -> "", "sample.conf" -> "conf")
      .map(f => f._1 -> f._2.getBytes(StandardCharsets.UTF_8)) ++
      Map("binary-file.conf" -> Array[Byte](0x00.toByte, 0xA1.toByte))
    val sparkConf = testSetup(input)
    val output = KubernetesClientUtils.loadSparkConfDirFiles(sparkConf)
    val expectedOutput = Map("test.txt" -> "test123", "sample.conf" -> "conf", "_test" -> "")
    assert(output === expectedOutput)
  }

  test("SPARK-54694: loadSparkConfDirFiles no longer silently truncates/skips files " +
      "when their combined size would exceed the config map max size") {
    // Before SPARK-54694, files were silently dropped here to stay under maxSize,
    // which could leave an application running with missing configuration. Now, all
    // conf dir files are loaded as-is; enforcing the size limit is the responsibility
    // of `validateConfigMapSize`, called right before the config map is actually built.
    val input = (for (i <- 10000 to 1 by -1) yield (s"testConf.${i}" -> "test123456")).toMap
    val sparkConf = testSetup(input.map(f => f._1 -> f._2.getBytes(StandardCharsets.UTF_8)))
      .set(Config.CONFIG_MAP_MAXSIZE.key, "60")
    val output = KubernetesClientUtils.loadSparkConfDirFiles(sparkConf)
    assert(output === input)
  }

  test("SPARK-54694: validateConfigMapSize fails fast when config map data exceeds maxSize") {
    val confFileMap = Map("testConf.1" -> "test123456", "testConf.2" -> "test123456")
    val sparkConf = new SparkConf(loadDefaults = false)
      .set(Config.CONFIG_MAP_MAXSIZE.key, "10")
    val ex = intercept[IllegalArgumentException] {
      KubernetesClientUtils.validateConfigMapSize(confFileMap, sparkConf)
    }
    assert(ex.getMessage.contains("exceeds the maximum config map size"))
  }

  test("SPARK-54694: validateConfigMapSize allows config map data within maxSize") {
    val confFileMap = Map("testConf.1" -> "test123456", "testConf.2" -> "test123456")
    val exactSize = confFileMap.map { case (k, v) => k.length + v.length }.sum
    val sparkConf = new SparkConf(loadDefaults = false)
      .set(Config.CONFIG_MAP_MAXSIZE.key, exactSize.toString)
    // Should not throw: data size exactly at the limit is allowed.
    KubernetesClientUtils.validateConfigMapSize(confFileMap, sparkConf)
  }

  test("SPARK-54694: validateConfigMapSize accounts for the resolved spark.properties size") {
    // Reproduces the second defect described in SPARK-54694: the size of the
    // resolved spark.properties file was not previously included when checking
    // the config map size limit, since that check only ran over the raw conf
    // dir files before spark.properties was merged in.
    val configMapName = s"configmap-name-${UUID.randomUUID.toString}"
    val sparkConf = testSetup(Map.empty)
    val resolvedProperties = Map("spark.testConf" -> ("v" * 1000))
    val confFileMap = KubernetesClientUtils.buildSparkConfDirFilesMap(
      configMapName, sparkConf, resolvedProperties)
    assert(confFileMap.contains(Constants.SPARK_CONF_FILE_NAME))
    val ex = intercept[IllegalArgumentException] {
      KubernetesClientUtils.validateConfigMapSize(
        confFileMap, sparkConf.set(Config.CONFIG_MAP_MAXSIZE.key, "10"))
    }
    assert(ex.getMessage.contains("exceeds the maximum config map size"))
  }

  test("verify that configmap built as expected") {
    val configMapName = s"configmap-name-${UUID.randomUUID.toString}"
    val configMapNameSpace = s"configmap-namespace-${UUID.randomUUID.toString}"
    val properties = Map(Config.KUBERNETES_NAMESPACE.key -> configMapNameSpace)
    val sparkConf =
      testSetup(properties.map(f => f._1 -> f._2.getBytes(StandardCharsets.UTF_8)))
    val confFileMap =
      KubernetesClientUtils.buildSparkConfDirFilesMap(configMapName, sparkConf, properties)
    val outputConfigMap =
      KubernetesClientUtils.buildConfigMap(configMapName, confFileMap, properties)
    val expectedConfigMap =
      new ConfigMapBuilder()
        .withNewMetadata()
          .withName(configMapName)
          .withNamespace(configMapNameSpace)
          .withLabels(properties.asJava)
        .endMetadata()
        .withImmutable(true)
        .addToData(confFileMap.asJava)
        .build()
    assert(outputConfigMap === expectedConfigMap)
  }

  test("SPARK-53832: verify that configmap built as expected va Java-friendly APIs") {
    val configMapName = s"configmap-name-${UUID.randomUUID.toString}"
    val configMapNameSpace = s"configmap-namespace-${UUID.randomUUID.toString}"
    val properties = new JHashMap[String, String]()
    properties.put(Config.KUBERNETES_NAMESPACE.key, configMapNameSpace)
    val sparkConf =
      testSetup(properties.asScala.toMap.map(f => f._1 -> f._2.getBytes(StandardCharsets.UTF_8)))
    val confFileMap =
      KubernetesClientUtils.buildSparkConfDirFilesMapJava(configMapName, sparkConf, properties)
    val outputConfigMap =
      KubernetesClientUtils.buildConfigMapJava(configMapName, confFileMap, properties)
    val expectedConfigMap =
      new ConfigMapBuilder()
        .withNewMetadata()
          .withName(configMapName)
          .withNamespace(configMapNameSpace)
          .withLabels(properties)
        .endMetadata()
        .withImmutable(true)
        .addToData(confFileMap)
        .build()
    assert(outputConfigMap === expectedConfigMap)
  }
}
