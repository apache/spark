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

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito.{mock, verify}
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker._

class UDFDispatcherManagerSuite
    extends AnyFunSuite { // scalastyle:ignore funsuite

  private def createManager(): (
      UDFDispatcherManager,
      ArrayBuffer[WorkerDispatcher]) = {
    val createdDispatchers = ArrayBuffer[WorkerDispatcher]()
    val factory = new UDFDispatcherFactory {
      override def createDispatcher(
          workerSpec: UDFWorkerSpecification,
          logger: WorkerLogger): WorkerDispatcher = {
        val dispatcher = mock(classOf[WorkerDispatcher])
        createdDispatchers += dispatcher
        dispatcher
      }
    }
    (new UDFDispatcherManager(factory), createdDispatchers)
  }

  private def makeSpec(
      command: String): UDFWorkerSpecification = {
    val callable = ProcessCallable.newBuilder()
    callable.addCommand(command)
    val caps = WorkerCapabilities.newBuilder()
      .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
      .addSupportedCommunicationPatterns(
        UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING)
    val conn = WorkerConnectionSpec.newBuilder()
      .setTcp(LocalTcpConnection.newBuilder())
    val props = UDFWorkerProperties.newBuilder()
      .setConnection(conn)
    val direct = DirectWorker.newBuilder()
      .setRunner(callable).setProperties(props)
    UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(caps).setDirect(direct).build()
  }

  test("Same spec returns the same dispatcher") {
    val (manager, createdDispatchers) = createManager()
    val spec = makeSpec("worker.bin")

    val d1 = manager.getDispatcher(spec)
    val d2 = manager.getDispatcher(spec)

    assert(d1 eq d2)
    assert(createdDispatchers.size === 1)
  }

  test("Structurally equal specs return the same dispatcher") {
    val (manager, createdDispatchers) = createManager()
    val spec1 = makeSpec("worker.bin")
    val spec2 = makeSpec("worker.bin")
    assert(spec1 ne spec2)
    assert(spec1 == spec2)

    val d1 = manager.getDispatcher(spec1)
    val d2 = manager.getDispatcher(spec2)

    assert(d1 eq d2)
    assert(createdDispatchers.size === 1)
  }

  test("Different specs create different dispatchers") {
    val (manager, createdDispatchers) = createManager()

    val dA = manager.getDispatcher(makeSpec("worker-a.bin"))
    val dB = manager.getDispatcher(makeSpec("worker-b.bin"))

    assert(dA ne dB)
    assert(createdDispatchers.size === 2)
  }

  test("Close closes all cached dispatchers") {
    val (manager, createdDispatchers) = createManager()

    manager.getDispatcher(makeSpec("worker-a.bin"))
    manager.getDispatcher(makeSpec("worker-b.bin"))
    manager.close()

    createdDispatchers.foreach(
      dispatcher => verify(dispatcher).close())
  }

  test("Close is idempotent") {
    val (manager, createdDispatchers) = createManager()
    manager.getDispatcher(makeSpec("worker.bin"))

    manager.close()
    manager.close()

    // close() should only be called once per dispatcher
    createdDispatchers.foreach(
      dispatcher => verify(dispatcher).close())
  }

  test("getDispatcher throws after close") {
    val (manager, _) = createManager()
    manager.close()

    intercept[IllegalStateException] {
      manager.getDispatcher(makeSpec("worker.bin"))
    }
  }
}
