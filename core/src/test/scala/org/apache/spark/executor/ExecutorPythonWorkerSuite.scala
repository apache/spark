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

import java.io.{DataInputStream, DataOutputStream, File}
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashMap

import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkFiles, SparkFunSuite}
import org.apache.spark.api.python.{LocalPythonWorkerHandle, PythonUtils, PythonWorker, PythonWorkerUtils}
import org.apache.spark.internal.config.Python.PYTHON_UNIX_DOMAIN_SOCKET_ENABLED
import org.apache.spark.util.{MutableURLClassLoader, Utils}

class ExecutorPythonWorkerSuite extends SparkFunSuite with SharedSparkContext {

  // Use socket streams with a read timeout for the probe's blocking request/response protocol.
  conf.set(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED, false)

  private implicit val formats: DefaultFormats.type = DefaultFormats

  private def newSession(uuid: String): IsolatedSessionState = {
    val loader = new MutableURLClassLoader(Array.empty, getClass.getClassLoader)
    val session = new IsolatedSessionState(
      uuid, loader, loader, new HashMap[String, Long], new HashMap[String, Long],
      new HashMap[String, Long], None)
    IsolatedSessionState.sessions.put(uuid, session)
    assert(session.acquire())
    session
  }

  private def probe(worker: PythonWorker, uuid: String, artifact: String): JValue = {
    worker.selectionKey.cancel()
    worker.selector.close()
    worker.channel.configureBlocking(true)
    worker.refresh()
    val socket = worker.channel.socket()
    socket.setSoTimeout(10000)
    val out = new DataOutputStream(socket.getOutputStream)
    // Exercise the production per-task SparkFiles initialization, not just os.getcwd().
    PythonWorkerUtils.writeSparkFiles(Some(uuid), Set.empty, out)
    PythonWorkerUtils.writeUTF(artifact, out)
    out.flush()
    parse(PythonWorkerUtils.readUTF(new DataInputStream(socket.getInputStream)))
  }

  private def checkArtifacts(result: JValue, directory: File, content: String): Unit = {
    withClue(s"Worker probe: $result") {
      assert((result \ "cwd").extract[String] == directory.getCanonicalPath)
      assert((result \ "same_directory").extract[Boolean])
      assert((result \ "relative_artifact").extract[String] == content)
      assert((result \ "spark_files_artifact").extract[String] == content)
    }
  }

  gridTest("SPARK-54055/SPARK-50118: recreated session artifacts")(Seq(true, false)) {
    reuseWorker =>
      assume(!Utils.isWindows)
      val uuid = UUID.randomUUID().toString
      val directory = new File(SparkFiles.getRootDirectory(), uuid)
      val artifact = "artifact.txt"
      val pythonPath = Seq(
        getTestResourceFile("python/pyspark_worker_cwd.py").getParent,
        getWorkspaceFilePath("python").toString,
        getWorkspaceFilePath("python", "lib", PythonUtils.PY4J_ZIP_NAME).toString)
        .mkString(File.pathSeparator)
      val envVars = Map("SPARK_JOB_ARTIFACT_UUID" -> uuid, "PYTHONPATH" -> pythonPath) ++
        (if (reuseWorker) Map("SPARK_REUSE_WORKER" -> "1") else Map.empty[String, String])
      val env = SparkEnv.get
      val pythonExec = PythonUtils.defaultPythonExec
      val workerModule = "pyspark_worker_cwd"
      var session: IsolatedSessionState = null
      var worker: PythonWorker = null
      try {
        session = newSession(uuid)
        Files.createDirectories(directory.toPath)
        Files.writeString(new File(directory, artifact).toPath, "before cleanup")
        val (created, handle) = env.createPythonWorker(
          pythonExec, workerModule, envVars, useDaemon = true)
        worker = created
        val child = ProcessHandle.of(handle.get.asInstanceOf[LocalPythonWorkerHandle].pid).get()
        val daemon = child.parent().get()
        checkArtifacts(probe(worker, uuid, artifact), directory, "before cleanup")
        if (reuseWorker) {
          // Leave a live worker in the pool, still attached to the old working directory.
          env.releasePythonWorker(pythonExec, workerModule, envVars, worker)
          assert(child.isAlive)
        } else {
          // Even a fresh child inherits a surviving daemon's deleted working directory.
          env.destroyPythonWorker(pythonExec, workerModule, envVars, worker)
          child.onExit().get(10, TimeUnit.SECONDS)
        }
        assert(daemon.isAlive)
        session.markEvicted()
        session.release()
        session = null
        worker = null
        assert(!directory.exists())
        assert(!IsolatedSessionState.sessions.containsKey(uuid))

        // Simulate a later task obtaining the same session and re-downloading its artifacts.
        session = newSession(uuid)
        Files.createDirectories(directory.toPath)
        Files.writeString(new File(directory, artifact).toPath, "after cleanup")
        worker = env.createPythonWorker(pythonExec, workerModule, envVars, useDaemon = true)._1
        checkArtifacts(probe(worker, uuid, artifact), directory, "after cleanup")
      } finally {
        if (worker != null) {
          worker.stop()
        }
        // Also retire factories if the regression fails before session cleanup reaches them.
        env.removeExecutorPythonWorkerFactories(uuid)
        if (session != null) {
          session.markEvicted()
          session.release()
        }
      }
  }
}
