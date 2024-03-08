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

package org.apache.spark.deploy.k8s

import java.io.File
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, EnvVarSourceBuilder, PodBuilder}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkException, SparkFunSuite}

class KubernetesUtilsSuite extends SparkFunSuite with PrivateMethodTester {
  private val HOST = "test-host"
  private val POD = new PodBuilder()
    .withNewSpec()
    .withHostname(HOST)
    .withContainers(
      new ContainerBuilder().withName("first").build(),
      new ContainerBuilder().withName("second").build())
    .endSpec()
    .build()

  test("Selects the given container as spark container.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Some("second"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("first"))
    assert(sparkPod.container.getName == "second")
  }

  test("Selects the first container if no container name is given.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Option.empty)
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("second"))
    assert(sparkPod.container.getName == "first")
  }

  test("Falls back to the first container if given container name does not exist.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Some("does-not-exist"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("second"))
    assert(sparkPod.container.getName == "first")
  }

  test("constructs spark pod correctly with pod template with no containers") {
    val noContainersPod = new PodBuilder(POD).editSpec().withContainers().endSpec().build()
    val sparkPod = KubernetesUtils.selectSparkContainer(noContainersPod, Some("does-not-exist"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.container.getName == null)
    val sparkPodWithNoContainerName =
      KubernetesUtils.selectSparkContainer(noContainersPod, Option.empty)
    assert(sparkPodWithNoContainerName.pod.getSpec.getHostname == HOST)
    assert(sparkPodWithNoContainerName.container.getName == null)
  }

  test("SPARK-38201: check uploadFileToHadoopCompatibleFS with different delSrc and overwrite") {
    withTempDir { srcDir =>
      withTempDir { destDir =>
        val upload = PrivateMethod[Unit](Symbol("uploadFileToHadoopCompatibleFS"))
        val fileName = "test.txt"
        val srcFile = new File(srcDir, fileName)
        val src = new Path(srcFile.getAbsolutePath)
        val dest = new Path(destDir.getAbsolutePath, fileName)
        val fs = src.getFileSystem(new Configuration())

        def checkUploadException(delSrc: Boolean, overwrite: Boolean): Unit = {
          val message = intercept[SparkException] {
            KubernetesUtils.invokePrivate(upload(src, dest, fs, delSrc, overwrite))
          }.getMessage
          assert(message.contains("Error uploading file"))
        }

        def appendFileAndUpload(content: String, delSrc: Boolean, overwrite: Boolean): Unit = {
          FileUtils.write(srcFile, content, StandardCharsets.UTF_8, true)
          KubernetesUtils.invokePrivate(upload(src, dest, fs, delSrc, overwrite))
        }

        // Write a new file, upload file with delSrc = false and overwrite = true.
        // Upload successful and record the `fileLength`.
        appendFileAndUpload("init-content", delSrc = false, overwrite = true)
        val firstLength = fs.getFileStatus(dest).getLen

        // Append the file, upload file with delSrc = false and overwrite = true.
        // Upload succeeded but `fileLength` changed.
        appendFileAndUpload("append-content", delSrc = false, overwrite = true)
        val secondLength = fs.getFileStatus(dest).getLen
        assert(firstLength < secondLength)

        // Upload file with delSrc = false and overwrite = false.
        // Upload failed because dest exists and not changed.
        checkUploadException(delSrc = false, overwrite = false)
        assert(fs.exists(dest))
        assert(fs.getFileStatus(dest).getLen == secondLength)

        // Append the file again, upload file delSrc = true and overwrite = true.
        // Upload succeeded, `fileLength` changed and src not exists.
        appendFileAndUpload("append-content", delSrc = true, overwrite = true)
        val thirdLength = fs.getFileStatus(dest).getLen
        assert(secondLength < thirdLength)
        assert(!fs.exists(src))

        // Rewrite a new file, upload file with delSrc = true and overwrite = false.
        // Upload failed because dest exists, src still exists.
        FileUtils.write(srcFile, "re-init-content", StandardCharsets.UTF_8, true)
        checkUploadException(delSrc = true, overwrite = false)
        assert(fs.exists(src))
      }
    }
  }

  test("SPARK-38582: verify that envVars is built with kv env as expected") {
    val input = for (i <- 9 to 1 by -1) yield (s"testEnvKey.$i", s"testEnvValue.$i")
    val expectedEnvVars = (input :+ ("testKeyWithEmptyValue" -> "")).map { case(k, v) =>
      new EnvVarBuilder()
        .withName(k)
        .withValue(v).build()
    }
    val outputEnvVars =
      KubernetesUtils.buildEnvVars(input ++
        Seq("testKeyWithNullValue" -> null, "testKeyWithEmptyValue" -> ""))
    assert(outputEnvVars.toSet == expectedEnvVars.toSet)
  }

  test("SPARK-38582: verify that envVars is built with field ref env as expected") {
    val input = for (i <- 9 to 1 by -1) yield (s"testEnvKey.$i", s"v$i", s"testEnvValue.$i")
    val expectedEnvVars = input.map { env =>
      new EnvVarBuilder()
        .withName(env._1)
        .withValueFrom(new EnvVarSourceBuilder()
          .withNewFieldRef(env._2, env._3)
          .build())
        .build()
    }
    val outputEnvVars =
      KubernetesUtils.buildEnvVarsWithFieldRef(
        input ++ Seq(
          ("testKey1", null, "testValue1"),
          ("testKey2", "v1", null),
          ("testKey3", null, null)))
    assert(outputEnvVars == expectedEnvVars)
  }
}
