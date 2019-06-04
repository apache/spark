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

package org.apache.spark.executor


import java.io.File
import java.net.URL
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JavaFiles}
import java.nio.file.attribute.PosixFilePermission.{OWNER_EXECUTE, OWNER_READ, OWNER_WRITE}
import java.util.{EnumSet, Properties}

import scala.collection.mutable
import scala.concurrent.duration._

import com.google.common.io.Files
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}
import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.ResourceInformation
import org.apache.spark.ResourceName.GPU
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.LaunchTask
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{SerializableBuffer, Utils}

class CoarseGrainedExecutorBackendSuite extends SparkFunSuite
    with LocalSparkContext with MockitoSugar {

  private def writeFileWithJson(dir: File, strToWrite: JArray): String = {
    val f1 = File.createTempFile("test-resource-parser1", "", dir)
    JavaFiles.write(f1.toPath(), compact(render(strToWrite)).getBytes())
    f1.getPath()
  }

  test("parsing no resources") {
    val conf = new SparkConf
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)

    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)
    withTempDir { tmpDir =>
      val testResourceArgs: JObject = ("" -> "")
      val ja = JArray(List(testResourceArgs))
      val f1 = writeFileWithJson(tmpDir, ja)
      var error = intercept[SparkException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("Exception parsing the resources in"),
        s"Calling with no resources didn't error as expected, error: $error")
    }
  }

  test("parsing one resources") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)
    withTempDir { tmpDir =>
      val testResourceArgs =
        ("name" -> "gpu") ~
        ("addresses" -> Seq("0", "1"))
      val ja = JArray(List(testResourceArgs))
      val f1 = writeFileWithJson(tmpDir, ja)
      val parsedResources = backend.parseOrFindResources(Some(f1))

      assert(parsedResources.size === 1)
      assert(parsedResources.get("gpu").nonEmpty)
      assert(parsedResources.get("gpu").get.name === "gpu")
      assert(parsedResources.get("gpu").get.addresses.deep === Array("0", "1").deep)
    }
  }

  test("parsing multiple resources") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_SUFFIX, "3")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_SUFFIX, "3")
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("addresses" -> Seq("0", "1"))
      val fpgaArgs =
        ("name" -> "fpga") ~
          ("addresses" -> Seq("f1", "f2", "f3"))
      val ja = JArray(List(gpuArgs, fpgaArgs))
      val f1 = writeFileWithJson(tmpDir, ja)
      val parsedResources = backend.parseOrFindResources(Some(f1))

      assert(parsedResources.size === 2)
      assert(parsedResources.get("gpu").nonEmpty)
      assert(parsedResources.get("gpu").get.name === "gpu")
      assert(parsedResources.get("gpu").get.addresses.deep === Array("0", "1").deep)
      assert(parsedResources.get("fpga").nonEmpty)
      assert(parsedResources.get("fpga").get.name === "fpga")
      assert(parsedResources.get("fpga").get.addresses.deep === Array("f1", "f2", "f3").deep)
    }
  }

  test("error checking parsing resources and executor and task configs") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    // not enough gpu's on the executor
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("addresses" -> Seq("0"))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("Resource: gpu, with addresses: 0 is less than what the " +
        "user requested: 2"))
    }

    // missing resource on the executor
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "fpga") ~
          ("addresses" -> Seq("0"))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("Resource: gpu required but wasn't discovered on startup"))
    }
  }

  test("executor resource found less than required") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "4")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_SUFFIX, "1")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    // executor resources < required
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("addresses" -> Seq("0", "1"))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("gpu, with addresses: 0,1 is less than what the user requested: 4"))
    }
  }

  test("use discoverer") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_SUFFIX, "3")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_SUFFIX, "3")
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val fpgaDiscovery = new File(dir, "resourceDiscoverScriptfpga")
      Files.write("""echo '{"name": "fpga","addresses":["f1", "f2", "f3"]}'""",
        fpgaDiscovery, StandardCharsets.UTF_8)
      JavaFiles.setPosixFilePermissions(fpgaDiscovery.toPath(),
        EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
      conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_SUFFIX, fpgaDiscovery.getPath())

      val serializer = new JavaSerializer(conf)
      val env = createMockEnv(conf, serializer)

      // we don't really use this, just need it to get at the parser function
      val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1",
        4, Seq.empty[URL], env, None)

      val parsedResources = backend.parseOrFindResources(None)

      assert(parsedResources.size === 1)
      assert(parsedResources.get("fpga").nonEmpty)
      assert(parsedResources.get("fpga").get.name === "fpga")
      assert(parsedResources.get("fpga").get.addresses.deep === Array("f1", "f2", "f3").deep)
    }
  }

  test("track allocated resources by taskId") {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)
    val serializer = new JavaSerializer(conf)
    var backend: CoarseGrainedExecutorBackend = null

    try {
      val rpcEnv = RpcEnv.create("1", "localhost", 0, conf, securityMgr)
      val env = createMockEnv(conf, serializer, Some(rpcEnv))
      backend = new CoarseGrainedExecutorBackend(env.rpcEnv, rpcEnv.address.hostPort, "1",
        "host1", 4, Seq.empty[URL], env, None)
      assert(backend.taskResources.isEmpty)

      val taskId = 1000000
      // We don't really verify the data, just pass it around.
      val data = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
      val taskDescription = new TaskDescription(taskId, 2, "1", "TASK 1000000", 19, 1,
        mutable.Map.empty, mutable.Map.empty, new Properties,
        Map(GPU -> new ResourceInformation(GPU, Array("0", "1"))), data)
      val serializedTaskDescription = TaskDescription.encode(taskDescription)
      backend.executor = mock[Executor]
      backend.rpcEnv.setupEndpoint("Executor 1", backend)

      // Launch a new task shall add an entry to `taskResources` map.
      backend.self.send(LaunchTask(new SerializableBuffer(serializedTaskDescription)))
      eventually(timeout(10.seconds)) {
        assert(backend.taskResources.size == 1)
        assert(backend.taskResources(taskId)(GPU).addresses sameElements Array("0", "1"))
      }

      // Update the status of a running task shall not affect `taskResources` map.
      backend.statusUpdate(taskId, TaskState.RUNNING, data)
      assert(backend.taskResources.size == 1)
      assert(backend.taskResources(taskId)(GPU).addresses sameElements Array("0", "1"))

      // Update the status of a finished task shall remove the entry from `taskResources` map.
      backend.statusUpdate(taskId, TaskState.FINISHED, data)
      assert(backend.taskResources.isEmpty)
    } finally {
      if (backend != null) {
        backend.rpcEnv.shutdown()
      }
    }
  }

  private def createMockEnv(conf: SparkConf, serializer: JavaSerializer,
      rpcEnv: Option[RpcEnv] = None): SparkEnv = {
    val mockEnv = mock[SparkEnv]
    val mockRpcEnv = mock[RpcEnv]
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    when(mockEnv.rpcEnv).thenReturn(rpcEnv.getOrElse(mockRpcEnv))
    SparkEnv.set(mockEnv)
    mockEnv
  }
}
