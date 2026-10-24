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

import java.nio.channels.SocketChannel
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.jdk.CollectionConverters._

import org.mockito.Answers.RETURNS_DEEP_STUBS
import org.mockito.ArgumentMatchers.any
import org.mockito.MockedConstruction
import org.mockito.Mockito.{doAnswer, doThrow, mock, mockConstruction, never, times, verify, when}

import org.apache.spark.SparkEnv.PythonWorkersKey
import org.apache.spark.api.python.{PythonWorker, PythonWorkerFactory}
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.storage.BlockManager

class SparkEnvPythonWorkerSuite extends SparkFunSuite {

  private def key(
      uuid: String,
      driverOwned: Boolean = false,
      workerModule: String = "pyspark.worker"): PythonWorkersKey = {
    PythonWorkersKey("python3", workerModule, PythonWorkerFactory.defaultDaemonModule,
      Map("SPARK_JOB_ARTIFACT_UUID" -> uuid), driverOwned)
  }

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
          when(factory.hasActiveWorkers).thenReturn(false)
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

  test("SPARK-54055: executor session cleanup removes only matching factories") {
    withFactories { (env, factories) =>
      def create(envVars: Map[String, String], module: String = "pyspark.worker"): Unit = {
        env.createPythonWorker("python3", module, envVars, useDaemon = true)
      }

      create(key("first").envVars)
      create(key("first").envVars, "another.worker")
      create(key("second").envVars)
      create(key("default").envVars)
      create(Map.empty)
      val original = factories.constructed().asScala.toSeq
      assert(original.size == 5)

      env.removeExecutorPythonWorkerFactories("first")
      original.take(2).foreach(factory => verify(factory, times(1)).stop())
      original.drop(2).foreach(factory => verify(factory, never()).stop())
      // Repeated cleanup does not stop a factory twice.
      env.removeExecutorPythonWorkerFactories("first")
      original.take(2).foreach(factory => verify(factory, times(1)).stop())

      create(key("second").envVars)
      create(key("default").envVars)
      create(Map.empty)
      assert(factories.constructed().size() == 5)
      // A later task for an evicted session gets a fresh factory.
      create(key("first").envVars)
      assert(factories.constructed().size() == 6)
      verify(factories.constructed().get(5), never()).stop()
    }
  }

  test("SPARK-54055: executor and driver-owned factories are separate") {
    withFactories { (env, factories) =>
      // Even identical launch parameters must not share a factory across ownership scopes.
      env.createPythonWorker(key("session"), useDaemon = true)
      env.createPythonWorker(key("session", driverOwned = true), useDaemon = true)
      assert(factories.constructed().size() == 2)
      val Seq(executorFactory, plannerFactory) = factories.constructed().asScala.toSeq

      env.createPythonWorker(key("session", driverOwned = true), useDaemon = true)
      assert(factories.constructed().size() == 2)
      val worker = PythonWorker(null)
      env.releasePythonWorker(key("session", driverOwned = true), worker)
      verify(plannerFactory).releaseWorker(worker)
      env.destroyPythonWorker(key("session", driverOwned = true), worker)
      verify(plannerFactory).stopWorker(worker)
      verify(executorFactory, never()).releaseWorker(worker)
      verify(executorFactory, never()).stopWorker(worker)
    }
  }

  test("SPARK-54055: executor session cleanup keeps busy driver planners in local mode") {
    withFactories { (env, factories) =>
      env.createPythonWorker(key("session"), useDaemon = true)
      env.createPythonWorker(key("session", driverOwned = true), useDaemon = true)
      val Seq(executorFactory, plannerFactory) = factories.constructed().asScala.toSeq
      when(plannerFactory.hasActiveWorkers).thenReturn(true)

      env.removeExecutorPythonWorkerFactories("session")
      verify(executorFactory, times(1)).stop()
      verify(plannerFactory, never()).stop()

      // Once idle, a later executor cleanup retires the planner factory as well, so its daemon
      // does not keep running in the deleted session directory.
      when(plannerFactory.hasActiveWorkers).thenReturn(false)
      env.removeExecutorPythonWorkerFactories("session")
      verify(plannerFactory, times(1)).stop()
      env.createPythonWorker(key("session", driverOwned = true), useDaemon = true)
      assert(factories.constructed().size() == 3)
    }
  }

  test("SPARK-54055: driver session cleanup removes only that session's planner factories") {
    withFactories { (env, factories) =>
      env.createPythonWorker(key("session"), useDaemon = true)
      env.createPythonWorker(key("session", driverOwned = true), useDaemon = true)
      env.createPythonWorker(
        key("session", driverOwned = true, workerModule = "pyspark.sql.worker.analyze_udtf"),
        useDaemon = true)
      env.createPythonWorker(key("other", driverOwned = true), useDaemon = true)
      val Seq(executorFactory, planner1, planner2, otherPlanner) =
        factories.constructed().asScala.toSeq
      // The session is closed by the user, so stop its planners even if one is still busy.
      when(planner1.hasActiveWorkers).thenReturn(true)

      env.removeDriverPythonWorkerFactories("session")
      Seq(planner1, planner2).foreach(factory => verify(factory, times(1)).stop())
      Seq(executorFactory, otherPlanner).foreach(factory => verify(factory, never()).stop())
      env.removeDriverPythonWorkerFactories("session")
      Seq(planner1, planner2).foreach(factory => verify(factory, times(1)).stop())

      // Removed planner factories must not remain in the cache, while other factories do.
      env.createPythonWorker(key("session"), useDaemon = true)
      env.createPythonWorker(key("other", driverOwned = true), useDaemon = true)
      assert(factories.constructed().size() == 4)
      env.createPythonWorker(key("session", driverOwned = true), useDaemon = true)
      assert(factories.constructed().size() == 5)
    }
  }

  gridTest("SPARK-54055: workers of a removed factory are closed on release or destroy")(
      Seq(false, true)) { driverOwned =>
    withFactories { (env, factories) =>
      val workerKey = key("session", driverOwned)
      env.createPythonWorker(workerKey, useDaemon = true)
      val factory = factories.constructed().get(0)
      if (driverOwned) {
        env.removeDriverPythonWorkerFactories("session")
      } else {
        env.removeExecutorPythonWorkerFactories("session")
      }

      Seq(
        (w: PythonWorker) => env.releasePythonWorker(workerKey, w),
        (w: PythonWorker) => env.destroyPythonWorker(workerKey, w)
      ).foreach { releaseOrDestroy =>
        val channel = SocketChannel.open()
        val worker = PythonWorker(channel)
        try {
          channel.configureBlocking(false)
          worker.refresh()
          val selector = worker.selector
          val selectionKey = worker.selectionKey
          assert(channel.isOpen && selector.isOpen && selectionKey.isValid)

          releaseOrDestroy(worker)

          assert(!channel.isOpen)
          assert(!selector.isOpen)
          assert(!selectionKey.isValid)
        } finally {
          worker.stop()
        }
      }
      verify(factory, never()).releaseWorker(any())
      verify(factory, never()).stopWorker(any())
    }
  }

  gridTest("SPARK-54055: release and destroy use a still-registered factory")(
      Seq(false, true)) { driverOwned =>
    withFactories { (env, factories) =>
      val workerKey = key("session", driverOwned)
      env.createPythonWorker(workerKey, useDaemon = true)
      val factory = factories.constructed().get(0)
      val worker = mock(classOf[PythonWorker])

      env.releasePythonWorker(workerKey, worker)
      verify(factory, times(1)).releaseWorker(worker)
      env.destroyPythonWorker(workerKey, worker)
      verify(factory, times(1)).stopWorker(worker)
      verify(worker, never()).stop()
    }
  }

  test("SPARK-54055: SparkEnv shutdown stops every factory despite failures") {
    withFactories { (env, factories) =>
      env.createPythonWorker(key("session"), useDaemon = true)
      env.createPythonWorker(key("session", driverOwned = true), useDaemon = true)
      env.createPythonWorker(key("other"), useDaemon = true)
      val original = factories.constructed().asScala.toSeq
      doThrow(new RuntimeException("test factory stop failure")).when(original.head).stop()

      env.stop()
      original.foreach(factory => verify(factory, times(1)).stop())
      env.stop()
      original.foreach(factory => verify(factory, times(1)).stop())
    }
  }

  test("SPARK-54055: failure to stop one factory does not prevent session cleanup") {
    withFactories { (env, factories) =>
      Seq("first.worker", "second.worker").foreach { module =>
        env.createPythonWorker(key("session", workerModule = module), useDaemon = true)
      }
      val original = factories.constructed().asScala.toSeq
      doThrow(new RuntimeException("test factory stop failure")).when(original.head).stop()

      env.removeExecutorPythonWorkerFactories("session")
      original.foreach(factory => verify(factory, times(1)).stop())
      env.removeExecutorPythonWorkerFactories("session")
      original.foreach(factory => verify(factory, times(1)).stop())
      env.createPythonWorker(key("session", workerModule = "first.worker"), useDaemon = true)
      assert(factories.constructed().size() == 3)
    }
  }

  test("SPARK-54055: stopping a session's factories does not block other sessions") {
    withFactories { (env, factories) =>
      env.createPythonWorker(key("slow"), useDaemon = true)
      val slowFactory = factories.constructed().get(0)
      val stopping = new CountDownLatch(1)
      val resume = new CountDownLatch(1)
      doAnswer { _ =>
        stopping.countDown()
        assert(resume.await(10, TimeUnit.SECONDS))
        null
      }.when(slowFactory).stop()

      val cleanup = new Thread(() => env.removeExecutorPythonWorkerFactories("slow"))
      cleanup.start()
      try {
        assert(stopping.await(10, TimeUnit.SECONDS))
        // The factory is stopped outside the SparkEnv lock.
        env.createPythonWorker(key("other"), useDaemon = true)
        assert(factories.constructed().size() == 2)
      } finally {
        resume.countDown()
        cleanup.join(TimeUnit.SECONDS.toMillis(10))
      }
      assert(!cleanup.isAlive)
    }
  }
}
