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
}
