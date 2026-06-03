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
package org.apache.spark.sql.execution.externalUDF

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.SimplePythonFunction
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{TestDirectWorkerDispatcher,
  UnixSocketWorkerConnection}

/**
 * A test [[UnixSocketWorkerConnection]] that opens a real Unix
 * domain socket channel to the worker.
 */
private class RealSocketConnection(
    socketPath: String,
    private val channel: java.nio.channels.SocketChannel)
    extends UnixSocketWorkerConnection(socketPath) {

  override def isActive: Boolean = channel.isOpen

  override def close(): Unit = {
    channel.close()
    super.close()
  }
}

private object RealSocketConnection {
  def connect(socketPath: String): RealSocketConnection = {
    val address =
      java.net.UnixDomainSocketAddress.of(socketPath)
    val channel = java.nio.channels.SocketChannel.open(
      java.net.StandardProtocolFamily.UNIX)
    channel.connect(address)
    new RealSocketConnection(socketPath, channel)
  }
}

/**
 * Extends [[TestDirectWorkerDispatcher]] to use a real Unix
 * domain socket connection instead of just checking file
 * existence.
 */
private class RealSocketTestDispatcher(
    spec: UDFWorkerSpecification)
    extends TestDirectWorkerDispatcher(spec) {

  // Use /tmp to avoid UDS path length limits in deep
  // worktree paths.
  override protected def newEndpointAddress(
      workerId: String): String = {
    val socketDir = java.nio.file.Files.createTempDirectory(
      java.nio.file.Paths.get("/tmp"), "udf-test-")
    socketDir.toFile.deleteOnExit()
    socketDir.resolve(s"w-$workerId.sock").toString
  }

  override protected def createConnection(
      socketPath: String): UnixSocketWorkerConnection =
    RealSocketConnection.connect(socketPath)
}

/**
 * Tests that [[PythonUDFWorkerSpecification#fromPythonFunction]]
 * produces a valid [[UDFWorkerSpecification]] that can be used by
 * a [[org.apache.spark.udf.worker.core.WorkerDispatcher]]
 * to spawn a real Python worker process.
 *
 * The test overrides `spark.python.worker.module` to point to a
 * test-only Python module that creates the UDS socket and waits
 * for SIGTERM, verifying that pythonExec, PYTHONPATH, env vars,
 * and command construction all work end-to-end.
 */
class PythonUDFWorkerSpecificationSuite
    extends SharedSparkSession {

  import IntegratedUDFTestUtils.{
    isPySparkAvailable, pythonExec, pythonVer,
    pysparkPythonPath, pythonPath
  }

  // A test Python module that imports pyspark (validates
  // the environment), creates the UDS socket at the
  // --connection path, and waits for SIGTERM.
  // scalastyle:off line.size.limit
  private val testWorkerModuleSource =
    """
    |import argparse, signal, socket, sys, os
    |# Validate PySpark is importable
    |import pyspark
    |
    |parser = argparse.ArgumentParser()
    |parser.add_argument('--id', required=True)
    |parser.add_argument('--connection', required=True)
    |args = parser.parse_args()
    |
    |sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    |sock.bind(args.connection)
    |sock.listen(1)
    |
    |running = True
    |def handle_sigterm(signum, frame):
    |    global running
    |    running = False
    |signal.signal(signal.SIGTERM, handle_sigterm)
    |
    |while running:
    |    # Wait till we receive a signal
    |    signal.pause()
    |
    |sock.close()
    |try:
    |    os.unlink(args.connection)
    |except OSError:
    |    pass
    |""".stripMargin.trim
  // scalastyle:on line.size.limit

  /**
   * Writes the test worker module to a temp directory and
   * returns the directory path (to be added to PYTHONPATH)
   * and the module name.
   */
  private def createTestWorkerModule(): (File, String) = {
    val moduleName = "test_udf_worker"
    val moduleDir = Files.createTempDirectory(
      "udf-test-module-").toFile
    moduleDir.deleteOnExit()
    val moduleFile = new File(moduleDir, s"$moduleName.py")
    moduleFile.deleteOnExit()
    Files.write(moduleFile.toPath,
      testWorkerModuleSource.getBytes(StandardCharsets.UTF_8))
    (moduleDir, moduleName)
  }

  test("PythonUDFWorkerSpecification.fromPythonFunction" +
      " produces a spec that spawns a Python worker") {
    assume(isPySparkAvailable,
      "Python and PySpark must be available")

    val (moduleDir, moduleName) = createTestWorkerModule()

    // Create a PythonFunction with the test module dir
    // added to PYTHONPATH so the module is importable.
    val envVars = new java.util.HashMap[String, String]()
    envVars.put("PYTHONPATH",
      s"${moduleDir.getAbsolutePath}:" +
        s"$pysparkPythonPath:$pythonPath")
    val func = new SimplePythonFunction(
      command = Array.emptyByteArray,
      envVars = envVars,
      pythonIncludes = ArrayBuffer.empty[String].asJava,
      pythonExec = pythonExec,
      pythonVer = pythonVer,
      broadcastVars = null,
      accumulator = null)

    // Override the worker module config to use our test module
    val conf = spark.sparkContext.conf.clone()
    conf.set("spark.python.worker.module", moduleName)

    // Build the spec via the function under test
    val workerSpec =
      PythonUDFWorkerSpecification.fromPythonFunction(func, conf)

    // Verify the spec works end-to-end with a real
    // socket connection
    val dispatcher = new RealSocketTestDispatcher(workerSpec)
    try {
      val session = dispatcher.createSession(
        securityScope = None)
      assert(session != null,
        "Expected a non-null session from the dispatcher")
      session.close()
    } finally {
      dispatcher.close()
    }
  }
}
