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
import org.apache.spark.deploy.k8s.Config
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

  test("verify load files, truncates the content to maxSize, when keys are very large in number.") {
    val input = (for (i <- 10000 to 1 by -1) yield (s"testConf.${i}" -> "test123456")).toMap
    val sparkConf = testSetup(input.map(f => f._1 -> f._2.getBytes(StandardCharsets.UTF_8)))
      .set(Config.CONFIG_MAP_MAXSIZE.key, "60")
    val output = KubernetesClientUtils.loadSparkConfDirFiles(sparkConf)
    val expectedOutput = Map("testConf.1" -> "test123456", "testConf.2" -> "test123456")
    assert(output === expectedOutput)
    val output1 = KubernetesClientUtils.loadSparkConfDirFiles(
      sparkConf.set(Config.CONFIG_MAP_MAXSIZE.key, "250000"))
    assert(output1 === input)
  }

  test("verify load files, truncates the content to maxSize, when keys are equal in length.") {
    val input = (for (i <- 9 to 1 by -1) yield (s"testConf.${i}" -> "test123456")).toMap
    val sparkConf = testSetup(input.map(f => f._1 -> f._2.getBytes(StandardCharsets.UTF_8)))
      .set(Config.CONFIG_MAP_MAXSIZE.key, "80")
    val output = KubernetesClientUtils.loadSparkConfDirFiles(sparkConf)
    val expectedOutput = Map("testConf.1" -> "test123456", "testConf.2" -> "test123456",
      "testConf.3" -> "test123456")
    assert(output === expectedOutput)
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

  test("SPARK-56845: configMapName with custom suffix truncates correctly") {
    // Test that short names pass through unchanged
    val shortPrefix = "my-app"
    val suffix = "-hadoop-config"
    val shortResult = KubernetesClientUtils.configMapName(shortPrefix, suffix)
    assert(shortResult === s"$shortPrefix$suffix")

    // Test that names within limit pass through unchanged
    val longPrefix = "a" * 239  // 239 + 14 ("-hadoop-config") = 253, exactly at limit
    val longResult = KubernetesClientUtils.configMapName(longPrefix, suffix)
    assert(longResult.length === 253)
    assert(longResult === s"$longPrefix$suffix")

    // Test that names exceeding limit fall back to spark-<uniqueID><suffix>
    val veryLongPrefix = "a" * 240  // 240 + 14 = 254, exceeds limit
    val fallbackResult = KubernetesClientUtils.configMapName(veryLongPrefix, suffix)
    assert(fallbackResult.length <= 253)
    assert(fallbackResult.startsWith("spark-"))
    assert(fallbackResult.endsWith(suffix))
    // Verify it's not the original prefix
    assert(!fallbackResult.startsWith(veryLongPrefix))
  }

  test("SPARK-56845: configMapName with different suffixes") {
    val prefix = "my-spark-application"

    // Test with -hadoop-config suffix
    val hadoopResult = KubernetesClientUtils.configMapName(prefix, "-hadoop-config")
    assert(hadoopResult === s"$prefix-hadoop-config")

    // Test with -krb5-file suffix
    val krb5Result = KubernetesClientUtils.configMapName(prefix, "-krb5-file")
    assert(krb5Result === s"$prefix-krb5-file")

    // Test with -driver-podspec-conf-map suffix
    val podspecResult = KubernetesClientUtils.configMapName(prefix, "-driver-podspec-conf-map")
    assert(podspecResult === s"$prefix-driver-podspec-conf-map")
  }

  test("SPARK-56845: configMapName fallback when prefix+suffix exceeds limit") {
    // Create a prefix that when combined with suffix exceeds 253 chars
    val longPrefix = "a" * 230
    val suffix = "-hadoop-config" // 14 chars, total would be 244 chars (within limit)
    val result1 = KubernetesClientUtils.configMapName(longPrefix, suffix)
    assert(result1.length <= 253)
    assert(result1 === s"$longPrefix$suffix")

    // Create a prefix that definitely exceeds the limit with suffix
    val veryLongPrefix = "a" * 245
    val result2 = KubernetesClientUtils.configMapName(veryLongPrefix, suffix)
    assert(result2.length <= 253)
    assert(result2.startsWith("spark-"))
    assert(result2.endsWith(suffix))
    // Verify it's not the original prefix
    assert(!result2.startsWith(veryLongPrefix))
  }

  test("SPARK-56845: configMapName backward compatibility with default suffix") {
    val prefix = "my-app"
    // Test that the single-argument version still works
    val defaultResult = KubernetesClientUtils.configMapName(prefix)
    assert(defaultResult === s"$prefix-conf-map")

    // Test that it's equivalent to calling with explicit -conf-map suffix
    val explicitResult = KubernetesClientUtils.configMapName(prefix, "-conf-map")
    assert(defaultResult === explicitResult)
  }
}
