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

import java.nio._
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success, Try}

import org.apache.commons.io.IOUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.network.{BlockDataManager, BlockTransferService}
import org.apache.spark.storage.{BlockId, ShuffleBlockId}
import org.apache.spark.{SecurityManager, SparkConf}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, ShouldMatchers}

class NettyBlockTransferSecuritySuite extends FunSuite with MockitoSugar with ShouldMatchers {
  test("security default off") {
    testConnection(new SparkConf, new SparkConf) match {
      case Success(_) => // expected
      case Failure(t) => fail(t)
    }
  }

  test("security on same password") {
    val conf = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    testConnection(conf, conf) match {
      case Success(_) => // expected
      case Failure(t) => fail(t)
    }
  }

  test("security on mismatch password") {
    val conf0 = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    val conf1 = conf0.clone.set("spark.authenticate.secret", "bad")
    testConnection(conf0, conf1) match {
      case Success(_) => fail("Should have failed")
      case Failure(t) => t.getMessage should include ("Mismatched response")
    }
  }

  test("security mismatch auth off on server") {
    val conf0 = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    val conf1 = conf0.clone.set("spark.authenticate", "false")
    testConnection(conf0, conf1) match {
      case Success(_) => fail("Should have failed")
      case Failure(t) => // any funny error may occur, sever will interpret SASL token as RPC
    }
  }

  test("security mismatch auth off on client") {
    val conf0 = new SparkConf()
      .set("spark.authenticate", "false")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    val conf1 = conf0.clone.set("spark.authenticate", "true")
    testConnection(conf0, conf1) match {
      case Success(_) => fail("Should have failed")
      case Failure(t) => t.getMessage should include ("Expected SaslMessage")
    }
  }

  test("security mismatch app ids") {
    val conf0 = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    val conf1 = conf0.clone.set("spark.app.id", "other-id")
    testConnection(conf0, conf1) match {
      case Success(_) => fail("Should have failed")
      case Failure(t) => t.getMessage should include ("SASL appId app-id did not match")
    }
  }

  /**
   * Creates two servers with different configurations and sees if they can talk.
   * Returns Success() if they can transfer a block, and Failure() if the block transfer was failed
   * properly. We will throw an out-of-band exception if something other than that goes wrong.
   */
  private def testConnection(conf0: SparkConf, conf1: SparkConf): Try[Unit] = {
    val blockManager = mock[BlockDataManager]
    val blockId = ShuffleBlockId(0, 1, 2)
    val blockString = "Hello, world!"
    val blockBuffer = new NioManagedBuffer(ByteBuffer.wrap(blockString.getBytes))
    when(blockManager.getBlockData(blockId)).thenReturn(blockBuffer)

    val securityManager0 = new SecurityManager(conf0)
    val exec0 = new NettyBlockTransferService(conf0, securityManager0)
    exec0.init(blockManager)

    val securityManager1 = new SecurityManager(conf1)
    val exec1 = new NettyBlockTransferService(conf1, securityManager1)
    exec1.init(blockManager)

    val result = fetchBlock(exec0, exec1, "1", blockId) match {
      case Success(buf) =>
        IOUtils.toString(buf.createInputStream()) should equal(blockString)
        buf.release()
        Success()
      case Failure(t) =>
        Failure(t)
    }
    exec0.close()
    exec1.close()
    result
  }

  /** Synchronously fetches a single block, acting as the given executor fetching from another. */
  private def fetchBlock(
      self: BlockTransferService,
      from: BlockTransferService,
      execId: String,
      blockId: BlockId): Try[ManagedBuffer] = {

    val promise = Promise[ManagedBuffer]()

    self.fetchBlocks(from.hostName, from.port, execId, Array(blockId.toString),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          promise.failure(exception)
        }

        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          promise.success(data.retain())
        }
      })

    Await.ready(promise.future, FiniteDuration(1000, TimeUnit.MILLISECONDS))
    promise.future.value.get
  }
}

