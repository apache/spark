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

import java.util.concurrent.TimeUnit

import org.apache.commons.io.IOUtils
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, LargeByteBufferHelper, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.{SecurityManager, SparkConf}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSuite}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Promise}

class NettyBlockTransferSuite extends FunSuite with Matchers with MockitoSugar {

  val conf = new SparkConf()
    .set("spark.app.id", "app-id")
  val securityManager = new SecurityManager(conf)



  test("simple fetch") {

    val blockManager = mock[BlockDataManager]
    val blockId = ShuffleBlockId(0, 1, 2)
    val buf = LargeByteBufferHelper.allocate(Integer.MAX_VALUE.toLong + 100l)
    val blockBuffer = new NioManagedBuffer(buf)
    when(blockManager.getBlockData(blockId)).thenReturn(blockBuffer)

    val from = new NettyBlockTransferService(conf, securityManager, numCores = 1)
    from.init(blockManager)
    println("from: " + from.hostName + ":" + from.port)
    val to = new NettyBlockTransferService(conf, securityManager, numCores = 1)
    to.init(blockManager)
    println("to: " + to.hostName + ":" + to.port)

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
      val v = promise.future.value.get.get
//      IOUtils.toString(v.createInputStream()) should equal(blockString)
      println(v.nioByteBuffer().limit())
    } finally {
      from.close()
      to.close()
    }


  }




}
