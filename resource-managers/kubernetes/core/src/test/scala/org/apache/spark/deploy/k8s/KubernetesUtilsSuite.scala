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
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

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
    val upload = PrivateMethod[Unit](Symbol("uploadFileToHadoopCompatibleFS"))
    withTempDir { srcDir =>
      val fileName = "test.txt"
      val srcFile = new File(srcDir, fileName)
      FileUtils.write(srcFile, "test", Charset.defaultCharset())
      withTempDir { destDir =>
        val src = new Path(srcFile.getAbsolutePath)
        val dest = new Path(destDir.getAbsolutePath, fileName)
        val fs = src.getFileSystem(new Configuration())
        // Scenario 1: delSrc = false and overwrite = true, upload successful
        KubernetesUtils.invokePrivate(upload(src, dest, fs, false, true))
        val firstUploadTime = fs.getFileStatus(dest).getModificationTime
        // sleep 1s to ensure that the `ModificationTime` changes.
        TimeUnit.SECONDS.sleep(1)
        // Scenario 2: delSrc = false and overwrite = true,
        // upload succeeded but `ModificationTime` changed
        KubernetesUtils.invokePrivate(upload(src, dest, fs, false, true))
        val secondUploadTime = fs.getFileStatus(dest).getModificationTime
        assert(firstUploadTime != secondUploadTime)

        // Scenario 3: delSrc = false and overwrite = false,
        // upload failed because dest exists
        val message = intercept[SparkException] {
          KubernetesUtils.invokePrivate(upload(src, dest, fs, false, false))
        }.getMessage
        assert(message.contains("Error uploading file"))

        TimeUnit.SECONDS.sleep(1)
        // Scenario 4: delSrc = true and overwrite = true,
        // upload succeeded, `ModificationTime` changed and src not exists.
        KubernetesUtils.invokePrivate(upload(src, dest, fs, true, true))
        val thirdUploadTime = fs.getFileStatus(dest).getModificationTime
        assert(secondUploadTime != thirdUploadTime)
        assert(!fs.exists(src))
      }
    }
  }
}
