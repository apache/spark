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
package org.apache.spark.network.netty

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import org.apache.commons.io.IOUtils
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer._
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.{BlockId, StorageLevel, RDDBlockId, ShuffleBlockId}
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.mockito.ArgumentCaptor
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSuite}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Promise}

class NettyBlockTransferSuite extends FunSuite with Matchers with MockitoSugar with Logging {

  val conf = new SparkConf()
    .set("spark.app.id", "app-id")
  val securityManager = new SecurityManager(conf)


  def fetchBlock(buf: LargeByteBuffer): ManagedBuffer = {
    val blockManager = mock[BlockDataManager]
    val blockId = ShuffleBlockId(0, 1, 2)
    val blockBuffer = new NioManagedBuffer(buf)
    when(blockManager.getBlockData(blockId)).thenReturn(blockBuffer)

    val from = new NettyBlockTransferService(conf, securityManager, numCores = 1)
    from.init(blockManager)
    val to = new NettyBlockTransferService(conf, securityManager, numCores = 1)
    to.init(blockManager)

    try {
      val promise = Promise[ManagedBuffer]()

      to.fetchBlocks(from.hostName, from.port, "1", Array(blockId.toString),
        new BlockFetchingListener {
          override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
            promise.failure(exception)
          }

          override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
            promise.success(data.retain())
          }
        })

      Await.ready(promise.future, FiniteDuration(100, TimeUnit.SECONDS))
      promise.future.value.get.get
    } finally {
      from.close()
      to.close()
    }

  }

  ignore("simple fetch") {
    val blockString = "Hello, world!"
    val blockBuffer = LargeByteBufferHelper.asLargeByteBuffer(blockString.getBytes)
    val fetched = fetchBlock(blockBuffer)

    IOUtils.toString(fetched.createInputStream()) should equal(blockString)
  }


  def uploadBlock(buf: LargeByteBuffer) {

    val fromBlockManager = mock[BlockDataManager]
    val toBlockManager = mock[BlockDataManager]
    val blockId = RDDBlockId(0, 1)
    val blockBuffer = new NioManagedBuffer(buf)
    val level = StorageLevel.DISK_ONLY //doesn't matter

    val from = new NettyBlockTransferService(conf, securityManager, numCores = 1)
    from.init(fromBlockManager)
    val to = new NettyBlockTransferService(conf, securityManager, numCores = 1)
    logTrace("to block manager = " + toBlockManager)
    to.init(toBlockManager)

    from.uploadBlock(to.hostName, to.port, "exec-1", blockId, blockBuffer, level)
    //TODO how to get rid of this wait??
    Thread.sleep(1000)
    val bufferCaptor = ArgumentCaptor.forClass(classOf[ManagedBuffer])
    verify(toBlockManager).putBlockData(MockitoMatchers.eq(blockId), bufferCaptor.capture(),
      MockitoMatchers.eq(level))
    val putBuffer = bufferCaptor.getValue()
  }

  test("simple upload") {
    val buf = LargeByteBufferHelper.asLargeByteBuffer(Array[Byte](0,1,2,3))
    uploadBlock(buf)
  }


  test("giant upload") {
    val parts = (0 until 2).map{_ => ByteBuffer.allocate(Integer.MAX_VALUE - 100)}.toArray
    val buf = new WrappedLargeByteBuffer(parts)
    uploadBlock(buf)
  }



  def equivalentBuffers(exp: ManagedBuffer, act: ManagedBuffer): Unit = {
    equivalentBuffers(exp.nioByteBuffer(), act.nioByteBuffer())
  }

  def equivalentBuffers(exp: LargeByteBuffer, act: LargeByteBuffer): Unit = {
    assert(exp.capacity() === act.capacity())
    assert(exp.remaining() === act.remaining())
    while (exp.remaining() > 0) {
      assert(exp.get() === act.get())
    }

  }



}
