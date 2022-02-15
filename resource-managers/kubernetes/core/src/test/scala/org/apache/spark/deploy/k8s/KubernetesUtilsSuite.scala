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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder}
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
        // Write a new file, upload file with delSrc = false and overwrite = true.
        // Upload successful and record the `fileLength.
        FileUtils.write(srcFile, "init-content", StandardCharsets.UTF_8)
        KubernetesUtils.invokePrivate(upload(src, dest, fs, false, true))
        val firstLength = fs.getFileStatus(dest).getLen

        // Append the file, upload file with delSrc = false and overwrite = true.
        // Upload succeeded but `fileLength` changed.
        FileUtils.write(srcFile, "append-content", StandardCharsets.UTF_8, true)
        KubernetesUtils.invokePrivate(upload(src, dest, fs, false, true))
        val secondLength = fs.getFileStatus(dest).getLen
        assert(firstLength < secondLength)

        // Upload file with delSrc = false and overwrite = false.
        // Upload failed because dest exists.
        val message1 = intercept[SparkException] {
          KubernetesUtils.invokePrivate(upload(src, dest, fs, false, false))
        }.getMessage
        assert(message1.contains("Error uploading file"))

        // Append the file again, upload file delSrc = true and overwrite = true.
        // Upload succeeded, `fileLength` changed and src not exists.
        FileUtils.write(srcFile, "append-content", StandardCharsets.UTF_8, true)
        KubernetesUtils.invokePrivate(upload(src, dest, fs, true, true))
        val thirdLength = fs.getFileStatus(dest).getLen
        assert(secondLength < thirdLength)
        assert(!fs.exists(src))

        // Rewrite a new file, upload file with delSrc = true and overwrite = false.
        // Upload failed because dest exists and src still exists.
        FileUtils.write(srcFile, "re-init-content", StandardCharsets.UTF_8, true)
        val message2 = intercept[SparkException] {
          KubernetesUtils.invokePrivate(upload(src, dest, fs, true, false))
        }.getMessage
        assert(message2.contains("Error uploading file"))
        assert(fs.exists(src))
      }
    }
  }
}
