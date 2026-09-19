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
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class PythonWorkerHandleSuite extends SparkFunSuite {

  test("terminationDiagnostics reads the worker's faulthandler log only when a log dir is bound") {
    // The handle reads its own worker's faulthandler log at <faultHandlerLogDir>/<pid>; whether it
    // is surfaced is a per-worker policy bound once at construction (None means faulthandler off),
    // so terminationDiagnostics takes no argument.
    withTempDir { dir =>
      val pid = ProcessHandle.current().pid()
      Files.writeString(new File(dir, pid.toString).toPath, "py-traceback")
      val disabled = new LocalPythonWorkerHandle(ProcessHandle.current(), faultHandlerLogDir = None)
      assert(disabled.terminationDiagnostics().isEmpty)
      // Still present: the disabled handle above must not have consumed the log.
      val enabled =
        new LocalPythonWorkerHandle(ProcessHandle.current(), faultHandlerLogDir = Some(dir))
      assert(enabled.terminationDiagnostics().exists(_.contains("py-traceback")))
    }
  }

  test("terminationDiagnostics is empty when the worker wrote no faulthandler log") {
    withTempDir { dir =>
      // Log dir bound, but no <pid> file was written.
      val handle =
        new LocalPythonWorkerHandle(ProcessHandle.current(), faultHandlerLogDir = Some(dir))
      assert(handle.terminationDiagnostics().isEmpty)
    }
  }

  test("isAlive and destroy terminate the underlying process") {
    assume(!Utils.isWindows)
    val proc = new ProcessBuilder("sleep", "30").start()
    try {
      val handle = new LocalPythonWorkerHandle(proc.toHandle)
      assert(handle.isAlive())
      assert(handle.destroy()) // kill issued
      assert(proc.waitFor(10, TimeUnit.SECONDS)) // process actually reaped
      assert(!handle.isAlive())
    } finally {
      proc.destroyForcibly()
    }
  }

  test("of returns a live handle that observes the process dying") {
    assume(!Utils.isWindows)
    val proc = new ProcessBuilder("sleep", "30").start()
    try {
      // Capture the handle while the process is alive; asserting liveness on it after the kill
      // avoids the pid-reuse race that re-querying of(reapedPid) would hit if the OS recycles it.
      val handle = PythonWorkerHandle.of(proc.pid())
      assert(handle.isDefined)
      assert(handle.get.isAlive())
      proc.destroyForcibly()
      assert(proc.waitFor(10, TimeUnit.SECONDS))
      assert(!handle.get.isAlive())
    } finally {
      proc.destroyForcibly()
    }
  }
}
