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
    val blockString = "Hello, world!"
    val blockBuffer = new NioManagedBuffer(LargeByteBufferHelper.asLargeByteBuffer(blockString.getBytes))
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

      Await.ready(promise.future, FiniteDuration(1000, TimeUnit.MILLISECONDS))
    } finally {
      from.close()
      to.close()
    }


  }




}
