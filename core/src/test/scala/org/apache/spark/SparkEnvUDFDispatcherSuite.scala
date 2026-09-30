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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

import org.mockito.Answers.RETURNS_DEEP_STUBS
import org.mockito.Mockito.{mock, verify}

import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.storage.BlockManager
import org.apache.spark.udf.worker.{DirectWorker, UDFWorkerSpecification}
import org.apache.spark.udf.worker.core.{UDFDispatcherFactory, WorkerDispatcher, WorkerLogger}
import org.apache.spark.util.ThreadUtils

class SparkEnvUDFDispatcherSuite extends SparkFunSuite {

  private def spec = UDFWorkerSpecification.getDefaultInstance

  private def newEnv(
      conf: SparkConf,
      factory: Option[() => UDFDispatcherFactory] = None): SparkEnv = {
    new SparkEnv(
      SparkContext.DRIVER_IDENTIFIER,
      mock(classOf[RpcEnv]),
      null,
      null,
      null,
      mock(classOf[MapOutputTracker]),
      mock(classOf[BroadcastManager]),
      mock(classOf[BlockManager], RETURNS_DEEP_STUBS),
      null,
      mock(classOf[MetricsSystem]),
      mock(classOf[OutputCommitCoordinator]),
      conf) {
      override private[spark] def createUDFDispatcherFactory(): UDFDispatcherFactory = {
        factory.map(_()).getOrElse(super.createUDFDispatcherFactory())
      }
    }
  }

  test("an unset worker type fails with an actionable message") {
    val factory = SparkEnv.resolveUDFDispatcherFactory(new SparkConf(false))
    val e = intercept[UnsupportedOperationException] {
      factory.createDispatcher(spec, WorkerLogger.NoOp)
    }
    assert(e.getMessage.contains("WORKER_NOT_SET"))
    assert(e.getMessage.contains("UDFWorkerSpecification.worker"))
  }

  test("a direct worker requests the Spark-owned runtime") {
    val directSpec = UDFWorkerSpecification
      .newBuilder()
      .setDirect(DirectWorker.getDefaultInstance)
      .build()
    val factory = SparkEnv.resolveUDFDispatcherFactory(new SparkConf(false))
    val e = intercept[SparkException] {
      factory.createDispatcher(directSpec, WorkerLogger.NoOp)
    }
    assert(e.getMessage.contains("DIRECT"))
    assert(e.getMessage.contains("spark-udf-worker-grpc"))
    assert(e.getMessage.contains(SparkEnv.DIRECT_DISPATCHER_FACTORY_CLASS))
  }

  test("the removed dispatcher factory class setting is rejected without loading the class") {
    val key = "spark.test.dispatcherModeInitialized"
    System.clearProperty(key)
    try {
      val conf = new SparkConf(false)
        .set(SparkEnv.REMOVED_DISPATCHER_FACTORY_KEY, "org.apache.spark.NotADispatcherMode$")
      val e = intercept[SparkException] {
        SparkEnv.resolveUDFDispatcherFactory(conf)
      }
      assert(e.getMessage.contains(SparkEnv.REMOVED_DISPATCHER_FACTORY_KEY))
      assert(e.getMessage.contains("Custom dispatcher classes are not supported"))
      assert(System.getProperty(key) == null)
    } finally {
      System.clearProperty(key)
    }
  }

  test("stop waits for lazy dispatcher manager creation and closes created dispatchers") {
    val hooks = TestBlockingDispatcherFactory
    val env = newEnv(new SparkConf(false), Some(() => new TestBlockingDispatcherFactory))
    val pool = ThreadUtils.newDaemonFixedThreadPool(2, "udf-dispatcher-test")
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val creator = Future { Try(env.getExternalUDFDispatcher(spec)) }
    try {
      assert(hooks.entered.await(10, TimeUnit.SECONDS))
      val stopStarted = new CountDownLatch(1)
      val stopFinished = new CountDownLatch(1)
      val stopper = Future {
        stopStarted.countDown()
        env.stop()
        stopFinished.countDown()
      }
      assert(stopStarted.await(10, TimeUnit.SECONDS))
      assert(!stopFinished.await(100, TimeUnit.MILLISECONDS))
      hooks.release.countDown()
      val result = ThreadUtils.awaitResult(creator, 30.seconds)
      ThreadUtils.awaitResult(stopper, 30.seconds)
      assert(result.isSuccess || result.failed.get.isInstanceOf[IllegalStateException])
      if (hooks.dispatcherCreated) {
        verify(hooks.dispatcher).close()
      }
      intercept[IllegalStateException] {
        env.getExternalUDFDispatcher(spec)
      }
    } finally {
      hooks.release.countDown()
      pool.shutdownNow()
    }
  }
}

object NotADispatcherMode {
  System.setProperty("spark.test.dispatcherModeInitialized", "true")
}

object TestBlockingDispatcherFactory {
  val entered = new CountDownLatch(1)
  val release = new CountDownLatch(1)
  val dispatcher: WorkerDispatcher = mock(classOf[WorkerDispatcher])
  @volatile var dispatcherCreated = false
}

class TestBlockingDispatcherFactory extends UDFDispatcherFactory {
  TestBlockingDispatcherFactory.entered.countDown()
  if (!TestBlockingDispatcherFactory.release.await(30, TimeUnit.SECONDS)) {
    throw new IllegalStateException("Timed out waiting to create the test dispatcher factory")
  }

  override def createDispatcher(
      workerSpec: UDFWorkerSpecification,
      logger: WorkerLogger): WorkerDispatcher = {
    TestBlockingDispatcherFactory.dispatcherCreated = true
    TestBlockingDispatcherFactory.dispatcher
  }
}
