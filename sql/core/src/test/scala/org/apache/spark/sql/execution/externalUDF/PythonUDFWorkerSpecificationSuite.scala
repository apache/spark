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
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.SimplePythonFunction
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.udf.worker.{Cancel, UDFWorkerSpecification}
import org.apache.spark.udf.worker.core.WorkerConnection
import org.apache.spark.udf.worker.grpc.testing.TestDirectGrpcDispatcher

/**
 * A [[WorkerConnection]] that opens a real Unix-domain-socket channel to the
 * worker at construction and considers itself active while the channel is
 * open. Verifies the worker's socket is actually connectable, not merely
 * that the file exists.
 */
private class ConnectingSocketConnection(
    private val channel: SocketChannel) extends WorkerConnection {
  override def isActive: Boolean = channel.isOpen
  override def close(): Unit = channel.close()
}

private object ConnectingSocketConnection {
  def connect(socketPath: String): ConnectingSocketConnection = {
    val channel = SocketChannel.open(StandardProtocolFamily.UNIX)
    channel.connect(UnixDomainSocketAddress.of(socketPath))
    new ConnectingSocketConnection(channel)
  }
}

/**
 * A [[TestDirectGrpcDispatcher]] whose connection actually connects to the
 * worker socket (rather than just checking file existence), so the test
 * verifies the spawned Python worker is reachable.
 */
private class ConnectingTestDispatcher(spec: UDFWorkerSpecification)
    extends TestDirectGrpcDispatcher(spec) {
  override protected def newConnection(address: String): WorkerConnection =
    ConnectingSocketConnection.connect(address)
}

/**
 * Tests that [[PythonUDFWorkerSpecification#fromPythonFunction]]
 * produces a valid [[org.apache.spark.udf.worker.UDFWorkerSpecification]]
 * that can be used by a
 * [[org.apache.spark.udf.worker.core.WorkerDispatcher]]
 * to spawn a real Python worker process.
 *
 * The test overrides `spark.python.worker.module` to point to a
 * test-only Python module that creates the UDS socket and waits
 * for SIGTERM, verifying that pythonExec, PYTHONPATH, env vars,
 * and command construction all work end-to-end.
 *
 * Reuses [[TestDirectGrpcDispatcher]]'s spawn / wait-for-ready machinery
 * (shared from `udf-worker-grpc` test sources) but overrides the connection
 * to open a real UDS channel -- the Python test worker only binds a raw
 * socket, it does not host a gRPC server, so a full gRPC session is not
 * exercised here.
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

    // Verify the spec works end-to-end: the dispatcher spawns the Python
    // worker, waits for the socket, and opens a real UDS connection to it
    // (proving the worker is reachable, not just that the file exists).
    val dispatcher = new ConnectingTestDispatcher(workerSpec)
    try {
      val session = dispatcher.createSession(
        securityScope = None)
      assert(session != null,
        "Expected a non-null session from the dispatcher")
      session.close(() => Cancel.getDefaultInstance)
    } finally {
      dispatcher.close()
    }
  }
}
