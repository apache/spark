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
package org.apache.spark.sql.execution.python.streaming

import java.io.{DataInputStream, DataOutputStream}
import java.util.{ArrayList, HashMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark.SparkException
import org.apache.spark.api.python.{PythonFunction, SimplePythonFunction}
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.DriverStatefulProcessorHandleImpl
import org.apache.spark.sql.streaming.TimeMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class TransformWithStateInPySparkPreInitCleanupSuite extends SharedSparkSession {

  private val groupingKeySchema = StructType(Nil)

  private def newPythonFunction(): PythonFunction = {
    new SimplePythonFunction(
      command = Seq.empty[Byte],
      envVars = new HashMap[String, String](),
      pythonIncludes = new ArrayList[String](),
      pythonExec = "python3",
      pythonVer = "3",
      broadcastVars = null,
      accumulator = null)
  }

  private def newDriverHandle(): DriverStatefulProcessorHandleImpl = {
    new DriverStatefulProcessorHandleImpl(TimeMode.None(), null)
  }

  private class StubPreInitRunner(
      failInitWith: Option[Throwable] = None,
      failProcessWith: Option[Throwable] = None,
      failStopWith: Option[Throwable] = None)
      extends TransformWithStateInPySparkPythonPreInitRunner(
        newPythonFunction(),
        "pyspark.sql.streaming.transform_with_state_driver_worker",
        groupingKeySchema,
        newDriverHandle()) {

    val workerAlive = new AtomicBoolean(false)
    val initCount = new AtomicInteger(0)
    val processCount = new AtomicInteger(0)
    val stopCount = new AtomicInteger(0)

    override def init(): (DataOutputStream, DataInputStream) = {
      initCount.incrementAndGet()
      workerAlive.set(true)
      failInitWith.foreach(error => throw error)
      (null, null)
    }

    override def process(): Unit = {
      processCount.incrementAndGet()
      failProcessWith.foreach(error => throw error)
    }

    override def stop(): Unit = {
      stopCount.incrementAndGet()
      workerAlive.set(false)
      failStopWith.foreach(error => throw error)
    }
  }

  private def runPreInit(runner: TransformWithStateInPySparkPythonPreInitRunner): Unit = {
    TransformWithStateInPySparkExec.runPreInitRunner(runner)
  }

  test("init failure after worker creation still stops the runner") {
    val initFailure = new RuntimeException("init failed")
    val runner = new StubPreInitRunner(failInitWith = Some(initFailure))

    val thrown = intercept[Throwable] {
      runPreInit(runner)
    }

    assert(thrown eq initFailure)
    assert(runner.initCount.get() === 1)
    assert(runner.stopCount.get() === 1)
    assert(!runner.workerAlive.get())
    assert(runner.processCount.get() === 0)
  }

  test("repeated init failures do not accumulate live workers") {
    val runners = (1 to 20).map { _ =>
      val runner = new StubPreInitRunner(failInitWith = Some(new RuntimeException("init failed")))
      intercept[Throwable] {
        runPreInit(runner)
      }
      runner
    }

    assert(runners.forall(_.initCount.get() === 1))
    assert(runners.count(_.workerAlive.get()) === 0)
  }

  test("a stop failure does not mask the original init failure") {
    val initFailure = new RuntimeException("init failed")
    val stopFailure = new IllegalStateException("cleanup failed")
    val runner =
      new StubPreInitRunner(failInitWith = Some(initFailure), failStopWith = Some(stopFailure))

    val thrown = intercept[Throwable] {
      runPreInit(runner)
    }

    assert(thrown eq initFailure)
    assert(thrown.getSuppressed.contains(stopFailure))
    assert(runner.stopCount.get() === 1)
    assert(!runner.workerAlive.get())
  }

  test("process failure is wrapped but still stops the runner") {
    val processFailure = new RuntimeException("worker crashed")
    val runner = new StubPreInitRunner(failProcessWith = Some(processFailure))

    val thrown = intercept[SparkException] {
      runPreInit(runner)
    }

    assert(thrown.getMessage.contains("exited unexpectedly (crashed)"))
    assert(thrown.getCause eq processFailure)
    assert(runner.stopCount.get() === 1)
    assert(!runner.workerAlive.get())
  }

  test("success path stops the runner exactly once") {
    val runner = new StubPreInitRunner()

    runPreInit(runner)

    assert(runner.initCount.get() === 1)
    assert(runner.processCount.get() === 1)
    assert(runner.stopCount.get() === 1)
    assert(!runner.workerAlive.get())
  }

  test("stop before startStateServer ran does not throw NPE") {
    val runner = new TransformWithStateInPySparkPythonPreInitRunner(
      newPythonFunction(),
      "pyspark.sql.streaming.transform_with_state_driver_worker",
      groupingKeySchema,
      newDriverHandle())

    runner.stop()
    runner.stop()
  }
}
