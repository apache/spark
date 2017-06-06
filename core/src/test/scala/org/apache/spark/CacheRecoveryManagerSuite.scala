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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.CacheRecoveryManager._
import org.apache.spark.internal.config.DYN_ALLOCATION_CACHE_RECOVERY_TIMEOUT
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rpc._
import org.apache.spark.storage.{BlockId, BlockManagerId, RDDBlockId}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.ThreadUtils

class CacheRecoveryManagerSuite
  extends SparkFunSuite with MockitoSugar with Matchers {

  val oneGB: Long = ByteUnit.GiB.toBytes(1).toLong

  val plentyOfMem = Map(
    BlockManagerId("1", "host", 12, None) -> ((oneGB, oneGB)),
    BlockManagerId("2", "host", 12, None) -> ((oneGB, oneGB)),
    BlockManagerId("3", "host", 12, None) -> ((oneGB, oneGB)))

  test("replicate blocks until empty and then kill executor") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(2, 1))
    val bmme = FakeBMM(blocks.iterator, plentyOfMem)
    val bmmeRef = DummyRef(bmme)
    val cacheRecoveryManager = new CacheRecoveryManager(bmmeRef, eam, conf)
    when(eam.killExecutors(Seq("1"))).thenReturn(Seq("1"))

    try {
      val future = cacheRecoveryManager.startCacheRecovery(Seq("1"))
      val results = ThreadUtils.awaitResult(future, Duration(3, TimeUnit.SECONDS))
      results.head shouldBe DoneRecovering
      verify(eam).killExecutors(Seq("1"))
      bmme.replicated.get("1").get shouldBe 2
    } finally {
      cacheRecoveryManager.stop()
    }
  }

  test("kill executor if it takes too long to replicate") {
    val conf = new SparkConf().set(DYN_ALLOCATION_CACHE_RECOVERY_TIMEOUT.key, "1s")
    val eam = mock[ExecutorAllocationManager]
    val blocks = Set(RDDBlockId(1, 1), RDDBlockId(2, 1), RDDBlockId(3, 1), RDDBlockId(4, 1))
    val bmme = FakeBMM(blocks.iterator, plentyOfMem, pauseIndefinitely = true)
    val bmmeRef = DummyRef(bmme)
    val cacheRecoveryManager = new CacheRecoveryManager(bmmeRef, eam, conf)

    try {
      val future = cacheRecoveryManager.startCacheRecovery(Seq("1"))
      val results = ThreadUtils.awaitResult(future, Duration(3, TimeUnit.SECONDS))
      results.head shouldBe Timeout
      verify(eam, times(1)).killExecutors(Seq("1"))
      bmme.replicated.get("1").get shouldBe 1
    } finally {
      cacheRecoveryManager.stop()
    }
  }

  test("shutdown timer will get cancelled if replication finishes") {
    val conf = new SparkConf().set(DYN_ALLOCATION_CACHE_RECOVERY_TIMEOUT.key, "1s")
    val eam = mock[ExecutorAllocationManager]
    val blocks = Set(RDDBlockId(1, 1))
    val bmme = FakeBMM(blocks.iterator, plentyOfMem)
    val bmmeRef = DummyRef(bmme)
    val cacheRecoveryManager = new CacheRecoveryManager(bmmeRef, eam, conf)

    try {
      val future = cacheRecoveryManager.startCacheRecovery(Seq("1"))
      val results = ThreadUtils.awaitResult(future, Duration(3, TimeUnit.SECONDS))

      results.head shouldBe DoneRecovering
      verify(eam, times(1)).killExecutors(Seq("1"))
    } finally {
      cacheRecoveryManager.stop()
    }
  }

  test("blocks won't replicate if we are running out of space") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1))
    val memStatus = Map(BlockManagerId("1", "host", 12, None) -> ((2L, 1L)),
      BlockManagerId("2", "host", 12, None) -> ((3L, 1L)),
      BlockManagerId("3", "host", 12, None) -> ((4L, 1L)),
      BlockManagerId("4", "host", 12, None) -> ((4L, 4L)))
    val bmme = FakeBMM(blocks.iterator, memStatus)
    val bmmeRef = DummyRef(bmme)
    val cacheRecoveryManager = new CacheRecoveryManager(bmmeRef, eam, conf)

    try {
      val future = cacheRecoveryManager.startCacheRecovery(Seq("1", "2", "3"))
      val results = ThreadUtils.awaitResult(future, Duration(3, TimeUnit.SECONDS))
      results.foreach { _ shouldBe DoneRecovering }
      bmme.replicated.size shouldBe 2
    } finally {
      cacheRecoveryManager.stop()
    }
  }

  test("blocks won't replicate if we are stopping all executors") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1))
    val memStatus = Map(BlockManagerId("1", "host", 12, None) -> ((2L, 1L)),
      BlockManagerId("2", "host", 12, None) -> ((2L, 1L)),
      BlockManagerId("3", "host", 12, None) -> ((2L, 1L)),
      BlockManagerId("4", "host", 12, None) -> ((2L, 1L)))
    val bmme = FakeBMM(blocks.iterator, memStatus)
    val bmmeRef = DummyRef(bmme)
    val cacheRecoveryManager = new CacheRecoveryManager(bmmeRef, eam, conf)

    try {
      val future = cacheRecoveryManager.startCacheRecovery(Seq("1", "2", "3", "4"))
      val results = ThreadUtils.awaitResult(future, Duration(3, TimeUnit.SECONDS))
      results.foreach { _ shouldBe DoneRecovering }
      bmme.replicated.size shouldBe 0
    } finally {
      cacheRecoveryManager.stop()
    }
  }
}

private case class FakeBMM(
    blocks: Iterator[BlockId],
    memStatus: Map[BlockManagerId, (Long, Long)],
    sizeOfBlock: Long = 1,
    pauseIndefinitely: Boolean = false
  ) extends ThreadSafeRpcEndpoint {

  val rpcEnv: RpcEnv = null
  val replicated: ConcurrentHashMap[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RecoverLatestRDDBlock(execId, _) =>
      val future = Future {
        if (blocks.hasNext) {
          blocks.next()
          replicated.putIfAbsent(execId, new AtomicInteger(0))
          replicated.get(execId).incrementAndGet()
          true
        } else {
          false
        }
      }
      if (!pauseIndefinitely) { future.foreach(context.reply) }
    case GetMemoryStatus => context.reply(memStatus)
  }
}

// Turns an RpcEndpoint into RpcEndpointRef by calling receive and reply directly
private case class DummyRef(endpoint: RpcEndpoint) extends RpcEndpointRef(new SparkConf()) {
  def address: RpcAddress = null
  def name: String = null
  def send(message: Any): Unit = endpoint.receive(message)
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    val context = new DummyRpcCallContext[T]
    endpoint.receiveAndReply(context)(message)
    context.result
  }
}

// saves values you put in context.reply
private class DummyRpcCallContext[T] extends RpcCallContext {
  val promise: Promise[T] = Promise[T]()
  def result: Future[T] = promise.future
  def reply(response: Any): Unit = promise.success(response.asInstanceOf[T])
  def sendFailure(e: Throwable): Unit = ()
  def senderAddress: RpcAddress = null
}

