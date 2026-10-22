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

import scala.jdk.CollectionConverters._

import org.mockito.Answers.RETURNS_DEEP_STUBS
import org.mockito.MockedConstruction
import org.mockito.Mockito.{doThrow, mock, mockConstruction, never, times, verify, when}

import org.apache.spark.api.python.{PythonWorker, PythonWorkerFactory}
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.storage.BlockManager

class SparkEnvPythonWorkerSuite extends SparkFunSuite {

  private def withFactories(
      f: (SparkEnv, MockedConstruction[PythonWorkerFactory]) => Unit): Unit = {
    val env = new SparkEnv(
      SparkContext.DRIVER_IDENTIFIER, mock(classOf[RpcEnv]), null, null, null,
      mock(classOf[MapOutputTracker]), mock(classOf[BroadcastManager]),
      mock(classOf[BlockManager], RETURNS_DEEP_STUBS), null, mock(classOf[MetricsSystem]),
      mock(classOf[OutputCommitCoordinator]), new SparkConf(false))
    val factories = mockConstruction(classOf[PythonWorkerFactory],
      new MockedConstruction.MockInitializer[PythonWorkerFactory] {
        override def prepare(
            factory: PythonWorkerFactory,
            context: MockedConstruction.Context): Unit = {
          when(factory.useDaemonEnabled).thenReturn(true)
        }
      })
    try {
      f(env, factories)
    } finally {
      try {
        env.stop()
      } finally {
        factories.close()
      }
    }
  }

  test("SPARK-54055: session cleanup removes only matching Python worker factories") {
    withFactories { (env, factories) =>
      val first = Map("SPARK_JOB_ARTIFACT_UUID" -> "first")
      val second = Map("SPARK_JOB_ARTIFACT_UUID" -> "second")
      val default = Map("SPARK_JOB_ARTIFACT_UUID" -> "default")
      def create(envVars: Map[String, String], module: String = "pyspark.worker"): Unit = {
        env.createPythonWorker("python3", module, envVars, useDaemon = true)
      }

      create(first)
      create(first, "another.worker")
      create(second)
      create(default)
      create(Map.empty)
      val original = factories.constructed().asScala.toSeq
      assert(original.size == 5)

      env.removePythonWorkerFactories("first")
      original.take(2).foreach(factory => verify(factory, times(1)).stop())
      original.drop(2).foreach(factory => verify(factory, never()).stop())
      // Repeated cleanup does not stop a factory twice.
      env.removePythonWorkerFactories("first")
      original.take(2).foreach(factory => verify(factory, times(1)).stop())

      create(second)
      create(default)
      create(Map.empty)
      assert(factories.constructed().size() == 5)
      // A later task for an evicted session gets a fresh factory.
      create(first)
      assert(factories.constructed().size() == 6)
      verify(factories.constructed().get(5), never()).stop()
    }
  }

  test("SPARK-54055: session cleanup preserves driver-owned factories in local mode") {
    withFactories { (env, factories) =>
      val envVars = Map("SPARK_JOB_ARTIFACT_UUID" -> "session")
      val workerModule = "pyspark.sql.worker.analyze_udtf"
      val daemonModule = PythonWorkerFactory.defaultDaemonModule
      def create(driverOwned: Boolean): Unit = {
        env.createPythonWorker(
          "python3", workerModule, daemonModule, envVars,
          useDaemon = true, driverOwned = driverOwned)
      }

      // Even identical launch parameters must not share a factory across ownership scopes.
      create(driverOwned = false)
      create(driverOwned = true)
      val executorFactory = factories.constructed().get(0)
      val plannerFactory = factories.constructed().get(1)
      env.removePythonWorkerFactories("session")
      verify(executorFactory, times(1)).stop()
      verify(plannerFactory, never()).stop()

      create(driverOwned = true)
      assert(factories.constructed().size() == 2)
      val worker = PythonWorker(null)
      env.releasePythonWorker(
        "python3", workerModule, daemonModule, envVars, worker, driverOwned = true)
      verify(plannerFactory).releaseWorker(worker)
      env.destroyPythonWorker(
        "python3", workerModule, daemonModule, envVars, worker, driverOwned = true)
      verify(plannerFactory).stopWorker(worker)
      verify(executorFactory, never()).releaseWorker(worker)
      verify(executorFactory, never()).stopWorker(worker)

      create(driverOwned = false)
      assert(factories.constructed().size() == 3)
      env.removePythonWorkerFactories("session")
      verify(factories.constructed().get(2)).stop()
      verify(plannerFactory, never()).stop()
    }
  }

  test("SPARK-54055: SparkEnv shutdown stops both executor and driver-owned factories") {
    withFactories { (env, factories) =>
      val envVars = Map("SPARK_JOB_ARTIFACT_UUID" -> "session")
      Seq(false, true).foreach { driverOwned =>
        env.createPythonWorker(
          "python3", "pyspark.worker", PythonWorkerFactory.defaultDaemonModule, envVars,
          useDaemon = true, driverOwned = driverOwned)
      }
      assert(factories.constructed().size() == 2)
      env.stop()
      factories.constructed().asScala.foreach(factory => verify(factory, times(1)).stop())
      env.stop()
      factories.constructed().asScala.foreach(factory => verify(factory, times(1)).stop())
    }
  }

  test("SPARK-54055: failure to stop one factory does not prevent session cleanup") {
    withFactories { (env, factories) =>
      val envVars = Map("SPARK_JOB_ARTIFACT_UUID" -> "session")
      Seq("first.worker", "second.worker").foreach { module =>
        env.createPythonWorker("python3", module, envVars, useDaemon = true)
      }
      val original = factories.constructed().asScala.toSeq
      doThrow(new RuntimeException("test factory stop failure")).when(original.head).stop()

      env.removePythonWorkerFactories("session")
      original.foreach(factory => verify(factory, times(1)).stop())
      env.removePythonWorkerFactories("session")
      original.foreach(factory => verify(factory, times(1)).stop())
      env.createPythonWorker("python3", "first.worker", envVars, useDaemon = true)
      assert(factories.constructed().size() == 3)
    }
  }
}
