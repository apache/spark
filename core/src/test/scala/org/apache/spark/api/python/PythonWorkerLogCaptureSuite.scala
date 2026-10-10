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

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite

class PythonWorkerLogCaptureSuite extends SparkFunSuite {

  private val marker = "PYTHON_WORKER_LOGGING"

  // Drains the wrapped stream fully, emulating the RedirectThread that reads worker stdout.
  private def drain(in: InputStream): Unit = {
    val buf = new Array[Byte](256)
    while (in.read(buf) != -1) {}
    in.close()
  }

  private def sentinelBytes(pid: String): Array[Byte] =
    s"$marker:$pid:\n".getBytes(StandardCharsets.ISO_8859_1)

  test("sentinel count starts at zero and awaitLogsFlushed times out before any sentinel") {
    val capture = new PythonWorkerLogCapture("session-1")
    assert(capture.sentinelCount("1234") === 0L)
    // No sentinel processed yet, so waiting for one beyond baseline 0 must time out.
    assert(!capture.awaitLogsFlushed("1234", baseline = 0L, timeoutMs = 50L))
  }

  test("processing an end-of-logs sentinel advances the count and releases waiters") {
    val capture = new PythonWorkerLogCapture("session-1")
    val pid = "1234"

    // A single end-of-logs sentinel line (empty JSON payload) does not touch the BlockManager,
    // so it exercises the counting/await path without a live SparkEnv.
    drain(capture.wrapInputStream(new ByteArrayInputStream(sentinelBytes(pid))))

    assert(capture.sentinelCount(pid) === 1L)
    // A sentinel beyond baseline 0 is now visible; beyond baseline 1 it is not.
    assert(capture.awaitLogsFlushed(pid, baseline = 0L, timeoutMs = 1000L))
    assert(!capture.awaitLogsFlushed(pid, baseline = 1L, timeoutMs = 50L))
  }

  test("sentinels are tracked independently per worker id") {
    val capture = new PythonWorkerLogCapture("session-1")
    drain(capture.wrapInputStream(new ByteArrayInputStream(sentinelBytes("1111"))))

    assert(capture.sentinelCount("1111") === 1L)
    assert(capture.sentinelCount("2222") === 0L)
    assert(capture.awaitLogsFlushed("1111", baseline = 0L, timeoutMs = 1000L))
    assert(!capture.awaitLogsFlushed("2222", baseline = 0L, timeoutMs = 50L))
  }

  test("awaitLogsFlushed blocks until a concurrently produced sentinel arrives") {
    val capture = new PythonWorkerLogCapture("session-1")
    val pid = "9999"

    @volatile var flushed = false
    val waiter = new Thread(() => {
      flushed = capture.awaitLogsFlushed(pid, baseline = 0L, timeoutMs = 30000L)
    })
    waiter.start()

    // Give the waiter time to reach its wait() before feeding the sentinel from this thread.
    var attempts = 0
    while (attempts < 100 &&
      !(waiter.getState == Thread.State.TIMED_WAITING ||
        waiter.getState == Thread.State.WAITING)) {
      Thread.sleep(10)
      attempts += 1
    }
    drain(capture.wrapInputStream(new ByteArrayInputStream(sentinelBytes(pid))))

    waiter.join(30000L)
    assert(!waiter.isAlive)
    assert(flushed)
    assert(capture.sentinelCount(pid) === 1L)
  }
}
