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

package org.apache.spark.api.python

import java.net.SocketTimeoutException

import scala.collection.mutable
// scalastyle:off executioncontextglobal
import scala.concurrent.ExecutionContext.Implicits.global
// scalastyle:on executioncontextglobal
import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.spark.SharedSparkContext
import org.apache.spark.SparkException
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.ThreadUtils

// Tests for PythonWorkerFactory.
class PythonWorkerFactorySuite extends SparkFunSuite with SharedSparkContext {

  test("createSimpleWorker() fails with a timeout error if worker does not connect back") {
    // It verifies that server side times out in accept(), if the worker does not connect back.
    // E.g. the worker might fail at the beginning before it tries to connect back.

    val workerFactory = new PythonWorkerFactory(
      "python3", "pyspark.testing.non_existing_worker_module", Map.empty, false
    )

    // Create the worker in a separate thread so that if there is a bug where it does not
    // return (accept() used to be blocking), the test doesn't hang for a long time.
    val createFuture = Future {
      val ex = intercept[SparkException] {
        workerFactory.createSimpleWorker(blockingMode = true) // blockingMode doesn't matter.
        // NOTE: This takes 10 seconds (which is the accept timeout in PythonWorkerFactory).
        // That makes this a bit longish test.
      }
      assert(ex.getMessage.contains("Python worker failed to connect back"))
      assert(ex.getCause.isInstanceOf[SocketTimeoutException])
    }

    // Timeout ensures that the test fails in 5 minutes if createSimplerWorker() doesn't return.
    ThreadUtils.awaitReady(createFuture, 5.minutes)
  }

  test("idle worker pool is unbounded when idleWorkerMaxPoolSize is not set") {
    sc.conf.remove("spark.python.factory.idleWorkerMaxPoolSize")

    val factory = new PythonWorkerFactory("python3", "pyspark.worker", Map.empty, true)

    assert(factory.idleWorkers.size === 0)

    val mockWorkers: mutable.Queue[PythonWorker] = mutable.Queue.empty
    try {
      (1 to 3).foreach { _ =>
        val mockChannel = java.nio.channels.SocketChannel.open()
        mockChannel.configureBlocking(false)
        mockWorkers.enqueue(PythonWorker(mockChannel))
      }
      mockWorkers.foreach(factory.releaseWorker)
      assert(factory.idleWorkers.size === 3)

    } finally {
      mockWorkers.foreach(factory.stopWorker)
    }
  }

  test("idle worker pool is bounded when idleWorkerMaxPoolSize is set") {
    sc.conf.set("spark.python.factory.idleWorkerMaxPoolSize", "2")

    val factory = new PythonWorkerFactory("python3", "pyspark.worker", Map.empty, true)

    assert(factory.idleWorkers.size === 0)
    val mockWorkers: mutable.Queue[PythonWorker] = mutable.Queue.empty
    try {
      (1 to 2).foreach { _ =>
        val mockChannel = java.nio.channels.SocketChannel.open()
        mockChannel.configureBlocking(false)
        mockWorkers.enqueue(PythonWorker(mockChannel))
      }
      mockWorkers.foreach(factory.releaseWorker)
      assert(factory.idleWorkers.size === 2)


      val worker3 = {
        val mockChannel = java.nio.channels.SocketChannel.open()
        mockChannel.configureBlocking(false)
        PythonWorker(mockChannel)
      }
      mockWorkers.enqueue(worker3)
      factory.releaseWorker(worker3)
      assert(factory.idleWorkers.size === 2)
    } finally {
      mockWorkers.foreach(factory.stopWorker)
    }
  }
}

class PythonWorkerFactoryIdleSuite extends SparkFunSuite with SharedSparkContext {

  test("isIdleFactory returns false for default artifact UUID") {
    val factory = new PythonWorkerFactory(
      "python3", "pyspark.worker", Map.empty[String, String], true)
    try {
      assert(factory.jobArtifactUUID === "default")
      assert(!factory.isIdleFactory(0))
    } finally {
      factory.stop()
    }
  }

  test("isIdleFactory returns false for session factory with recent activity") {
    val envVars = Map("SPARK_JOB_ARTIFACT_UUID" -> "test-session-uuid")
    val factory = new PythonWorkerFactory(
      "python3", "pyspark.worker", envVars, true)
    try {
      assert(factory.jobArtifactUUID === "test-session-uuid")
      assert(!factory.isIdleFactory(java.util.concurrent.TimeUnit.HOURS.toNanos(1)))
    } finally {
      factory.stop()
    }
  }

  test("isIdleFactory returns true for session factory past timeout") {
    val envVars = Map("SPARK_JOB_ARTIFACT_UUID" -> "test-session-uuid")
    val factory = new PythonWorkerFactory(
      "python3", "pyspark.worker", envVars, true)
    try {
      assert(factory.jobArtifactUUID === "test-session-uuid")
      assert(factory.isIdleFactory(0))
    } finally {
      factory.stop()
    }
  }

  test("destroyPythonWorkersByArtifactUUID removes only matching factories") {
    val env = sc.env
    val uuid1 = "session-uuid-1"
    val uuid2 = "session-uuid-2"
    val envVars1 = Map("SPARK_JOB_ARTIFACT_UUID" -> uuid1)
    val envVars2 = Map("SPARK_JOB_ARTIFACT_UUID" -> uuid2)
    val defaultEnvVars = Map("SPARK_JOB_ARTIFACT_UUID" -> "default")

    // Access the internal cache via reflection
    val pythonWorkersField = env.getClass.getDeclaredField("pythonWorkers")
    pythonWorkersField.setAccessible(true)
    val rawMap = pythonWorkersField.get(env)
    val putMethod = rawMap.getClass.getMethod("put", classOf[Object], classOf[Object])
    val sizeMethod = rawMap.getClass.getMethod("size")
    def mapSize(): Int = sizeMethod.invoke(rawMap).asInstanceOf[Int]
    def factoryValues(): Iterable[PythonWorkerFactory] = {
      val valuesMethod = rawMap.getClass.getMethod("values")
      val values = valuesMethod.invoke(rawMap)
        .asInstanceOf[Iterable[PythonWorkerFactory]]
      values
    }

    val sizeBefore = mapSize()

    val factory1 = new PythonWorkerFactory("python3", "pyspark.worker", envVars1, true)
    val factory2 = new PythonWorkerFactory("python3", "pyspark.worker", envVars2, true)
    val factoryDefault = new PythonWorkerFactory(
      "python3", "pyspark.worker", defaultEnvVars, true)

    // Construct keys via reflection (PythonWorkersKey is private)
    val keyClass = env.getClass.getDeclaredClasses
      .find(_.getSimpleName.contains("PythonWorkersKey")).get
    val keyConstructor = keyClass.getDeclaredConstructors.head
    keyConstructor.setAccessible(true)
    def makeKey(envVars: Map[String, String]): AnyRef =
      keyConstructor.newInstance(
        env, "python3", "pyspark.worker",
        PythonWorkerFactory.defaultDaemonModule, envVars).asInstanceOf[AnyRef]

    val key1 = makeKey(envVars1)
    val key2 = makeKey(envVars2)
    val keyDefault = makeKey(defaultEnvVars)

    try {
      putMethod.invoke(rawMap, key1, factory1)
      putMethod.invoke(rawMap, key2, factory2)
      putMethod.invoke(rawMap, keyDefault, factoryDefault)
      assert(mapSize() === sizeBefore + 3)

      // Destroy factories for uuid1 only
      env.destroyPythonWorkersByArtifactUUID(uuid1)
      assert(mapSize() === sizeBefore + 2)
      assert(factoryValues().exists(_.jobArtifactUUID == uuid2))
      assert(factoryValues().exists(_.jobArtifactUUID == "default"))
      assert(!factoryValues().exists(_.jobArtifactUUID == uuid1))

      // Destroy factories for uuid2
      env.destroyPythonWorkersByArtifactUUID(uuid2)
      assert(mapSize() === sizeBefore + 1)
      assert(!factoryValues().exists(_.jobArtifactUUID == uuid2))
      assert(factoryValues().exists(_.jobArtifactUUID == "default"))
    } finally {
      val removeMethod = rawMap.getClass.getMethod("remove", classOf[Object])
      Seq(key1, key2, keyDefault).foreach { key =>
        val removed = removeMethod.invoke(rawMap, key)
        if (removed != null) {
          removed.asInstanceOf[Option[PythonWorkerFactory]].foreach(_.stop())
        }
      }
    }
  }
}
