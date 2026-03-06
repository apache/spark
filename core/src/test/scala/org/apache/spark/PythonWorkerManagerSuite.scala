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

package org.apache.spark

import org.apache.spark.api.python.PythonWorker

// Tests for PythonWorkerManager.
class PythonWorkerManagerSuite extends SparkFunSuite with SharedSparkContext {

  // A non-existent Python executable used to make create() fail fast (IOException on pb.start())
  // without the 10-second accept() timeout that a real-but-wrong module would incur.
  private val fakePython = "/nonexistent/python"

  test("destroyPythonWorker is a no-op when no factory is cached for the key") {
    val manager = new PythonWorkerManager
    val channel = java.nio.channels.SocketChannel.open()
    try {
      val worker = PythonWorker(channel)
      // Should not throw even though no factory exists for this key.
      manager.destroyPythonWorker(
        "python3",
        "pyspark.worker",
        "pyspark.daemon",
        Map.empty,
        worker)
    } finally {
      channel.close()
      manager.stop()
    }
  }

  test("releasePythonWorker is a no-op when no factory is cached for the key") {
    val manager = new PythonWorkerManager
    val channel = java.nio.channels.SocketChannel.open()
    try {
      val worker = PythonWorker(channel)
      // Should not throw even though no factory exists for this key.
      manager.releasePythonWorker(
        "python3",
        "pyspark.worker",
        "pyspark.daemon",
        Map.empty,
        worker)
    } finally {
      channel.close()
      manager.stop()
    }
  }

  test("stop() on an empty manager does not throw") {
    val manager = new PythonWorkerManager
    manager.stop()
  }

  test("createPythonWorker throws if useDaemon changes for a cached factory") {
    val manager = new PythonWorkerManager
    try {
      // First call: factory is inserted into the cache, then create() fails fast because
      // fakePython does not exist (IOException on ProcessBuilder.start()).
      intercept[Exception] {
        manager.createPythonWorker(
          fakePython,
          "pyspark.worker",
          "pyspark.daemon",
          Map.empty,
          useDaemon = false)
      }

      // Second call: same key hits the cached factory (useDaemonEnabled=false).
      // The consistency check fires immediately, before create() is called.
      val ex = intercept[SparkException] {
        manager.createPythonWorker(
          fakePython,
          "pyspark.worker",
          "pyspark.daemon",
          Map.empty,
          useDaemon = true)
      }
      assert(ex.getMessage.contains("not allowed to change"))
    } finally {
      manager.stop()
    }
  }
}
