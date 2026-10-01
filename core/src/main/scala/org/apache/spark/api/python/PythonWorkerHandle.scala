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

import java.io.File
import java.nio.file.{Files => JavaFiles}

/**
 * Abstracts the concrete process backend so the runner does not depend on the local OS process
 * directly, exposing only the operations it needs: liveness, termination, and post-mortem
 * diagnostics. [[LocalPythonWorkerHandle]] is the implementation backed by a local OS process.
 */
trait PythonWorkerHandle {
  def isAlive(): Boolean

  /** Best-effort terminate the worker; true iff a termination request was issued. */
  def destroy(): Boolean

  /**
   * Best-effort post-mortem diagnostics for a terminated worker, if any: the handle knows both how
   * to locate its own worker's diagnostics and whether they are enabled (bound when the handle was
   * created). Empty when disabled or none are available.
   */
  def terminationDiagnostics(): Option[String]
}

/**
 * Default [[PythonWorkerHandle]] backed by a local OS process.
 *
 * @param processHandle the JVM handle for the worker process
 * @param faultHandlerLogDir the directory the worker's Python faulthandler writes its log to, if
 *                           faulthandler is enabled for this worker; [[terminationDiagnostics]]
 *                           reads `<dir>/<pid>`. None means faulthandler is off, so no log is
 *                           surfaced. The creator binds this once, since it is fixed for the
 *                           worker's whole lifetime.
 */
class LocalPythonWorkerHandle(
    processHandle: ProcessHandle,
    faultHandlerLogDir: Option[File] = None)
  extends PythonWorkerHandle {

  /** The OS pid of the worker process; an implementation detail of this local backend. */
  def pid: Long = processHandle.pid()

  override def isAlive(): Boolean = processHandle.isAlive()

  override def destroy(): Boolean = processHandle.destroy()

  // The worker's faulthandler log lives at <faultHandlerLogDir>/<pid>; read it once, deleting it so
  // it is surfaced exactly once. None (faulthandler off) or a missing file yields no diagnostics.
  override def terminationDiagnostics(): Option[String] =
    faultHandlerLogDir
      .map(dir => new File(dir, pid.toString).toPath)
      .collect {
        case path if JavaFiles.exists(path) =>
          val error = String.join("\n", JavaFiles.readAllLines(path)) + "\n"
          JavaFiles.deleteIfExists(path)
          error
      }
}

object PythonWorkerHandle {

  /**
   * Create a [[LocalPythonWorkerHandle]] for the given pid, or None if the pid is not valid.
   *
   * @param faultHandlerLogDir the directory the worker's faulthandler writes to, or None if
   *                           faulthandler is off for this worker. Bound onto the handle and read
   *                           by [[LocalPythonWorkerHandle.terminationDiagnostics]].
   */
  def of(pid: Long, faultHandlerLogDir: Option[File] = None): Option[PythonWorkerHandle] = {
    val handle = ProcessHandle.of(pid)
    if (handle.isPresent) {
      Some(new LocalPythonWorkerHandle(handle.get, faultHandlerLogDir))
    } else {
      None
    }
  }
}
