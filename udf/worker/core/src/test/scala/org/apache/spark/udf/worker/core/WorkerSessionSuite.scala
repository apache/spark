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
package org.apache.spark.udf.worker.core

import java.util.concurrent.atomic.AtomicInteger

// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

private class TestWorkerSession
    extends WorkerSession(WorkerLogger.NoOp) {
  override protected def doInit(msg: InitMessage): Unit = {}
  override protected def doProcess(
      input: Iterator[Array[Byte]]): Iterator[Array[Byte]] =
    Iterator.empty
  override protected def doCancel(): Unit = {}
  override protected def doClose(): Unit = {}
}

class WorkerSessionSuite
    extends AnyFunSuite { // scalastyle:ignore funsuite

  test("Completion listener fires on close") {
    val session = new TestWorkerSession()
    val count = new AtomicInteger(0)
    session.addSessionCompletionListener(_ => count.incrementAndGet())

    session.close()
    assert(count.get() === 1)
  }

  test("Completion listener fires on cancel") {
    val session = new TestWorkerSession()
    val count = new AtomicInteger(0)
    session.addSessionCompletionListener(_ => count.incrementAndGet())

    session.cancel()
    assert(count.get() === 1)
  }

  test("Completion listener fires exactly once on close then cancel") {
    val session = new TestWorkerSession()
    val count = new AtomicInteger(0)
    session.addSessionCompletionListener(_ => count.incrementAndGet())

    session.close()
    session.cancel()
    assert(count.get() === 1)
  }

  test("Listener added after close fires immediately") {
    val session = new TestWorkerSession()
    session.close()

    val count = new AtomicInteger(0)
    session.addSessionCompletionListener(_ => count.incrementAndGet())
    assert(count.get() === 1)
  }

  test("Listener added after cancel fires immediately") {
    val session = new TestWorkerSession()
    session.cancel()

    val count = new AtomicInteger(0)
    session.addSessionCompletionListener(_ => count.incrementAndGet())
    assert(count.get() === 1)
  }

  test("Multiple listeners all fire exactly once") {
    val session = new TestWorkerSession()
    val count1 = new AtomicInteger(0)
    val count2 = new AtomicInteger(0)
    val count3 = new AtomicInteger(0)
    session.addSessionCompletionListener(_ => count1.incrementAndGet())
    session.addSessionCompletionListener(_ => count2.incrementAndGet())

    session.close()

    // Add a third after close
    session.addSessionCompletionListener(_ => count3.incrementAndGet())

    assert(count1.get() === 1)
    assert(count2.get() === 1)
    assert(count3.get() === 1)
  }

  test("Listener receives the correct session instance") {
    val session = new TestWorkerSession()
    var received: WorkerSession = null
    session.addSessionCompletionListener(s => received = s)

    session.close()
    assert(received eq session)
  }
}
