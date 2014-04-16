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

package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import scala.concurrent.Await

import akka.actor.{Actor, Props}
import akka.pattern.ask

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.scheduler.DeregisterReceiver
import org.apache.spark.streaming.scheduler.AddBlock
import scala.Some
import org.apache.spark.streaming.scheduler.RegisterReceiver

/**
 * Concrete implementation of [[org.apache.spark.streaming.receiver.NetworkReceiverExecutor]]
 * which provides all the necessary functionality for handling the data received by
 * the receiver. Specifically, it creates a [[org.apache.spark.streaming.receiver.BlockGenerator]]
 * object that is used to divide the received data stream into blocks of data.
 */
private[streaming] class NetworkReceiverExecutorImpl(
    receiver: NetworkReceiver[_],
    env: SparkEnv
  ) extends NetworkReceiverExecutor(receiver, env.conf) with Logging {

  private val blockManager = env.blockManager

  private val storageLevel = receiver.storageLevel

  /** Remote Akka actor for the NetworkInputTracker */
  private val trackerActor = {
    val ip = env.conf.get("spark.driver.host", "localhost")
    val port = env.conf.getInt("spark.driver.port", 7077)
    val url = "akka.tcp://spark@%s:%s/user/NetworkInputTracker".format(ip, port)
    env.actorSystem.actorSelection(url)
  }

  /** Timeout for Akka actor messages */
  private val askTimeout = AkkaUtils.askTimeout(env.conf)

  /** Akka actor for receiving messages from the NetworkInputTracker in the driver */
  private val actor = env.actorSystem.actorOf(
    Props(new Actor {
      override def preStart() {
        logInfo("Registered receiver " + receiverId)
        val msg = RegisterReceiver(
          receiverId, receiver.getClass.getSimpleName, Utils.localHostName(), self)
        val future = trackerActor.ask(msg)(askTimeout)
        Await.result(future, askTimeout)
      }

      override def receive() = {
        case StopReceiver =>
          logInfo("Received stop signal")
          stop("Stopped by driver")
      }
    }), "NetworkReceiver-" + receiverId + "-" + System.currentTimeMillis())

  /** Unique block ids if one wants to add blocks directly */
  private val newBlockId = new AtomicLong(System.currentTimeMillis())

  /** Divides received data records into data blocks for pushing in BlockManager. */
  private val blockGenerator = new BlockGenerator(new BlockGeneratorListener {
    def onError(message: String, throwable: Throwable) {
      reportError(message, throwable)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      pushArrayBuffer(arrayBuffer, None, Some(blockId))
    }
  }, receiverId, env.conf)

  /** Exceptions that occurs while receiving data */
  val exceptions = new ArrayBuffer[Exception] with SynchronizedBuffer[Exception]

  /** Push a single record of received data into block generator. */
  def pushSingle(data: Any) {
    blockGenerator += (data)
  }

  /** Push a block of received data as an ArrayBuffer into block generator. */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ) {
    val blockId = optionalBlockId.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    blockManager.put(blockId, arrayBuffer.asInstanceOf[ArrayBuffer[Any]],
      storageLevel, tellMaster = true)
    logDebug("Pushed block " + blockId + " in " + (System.currentTimeMillis - time)  + " ms")
    reportPushedBlock(blockId, arrayBuffer.size, optionalMetadata)
  }

  /** Push a block of received data as an iterator into block generator. */
  def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ) {
    val blockId = optionalBlockId.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    blockManager.put(blockId, iterator, storageLevel, tellMaster = true)
    logDebug("Pushed block " + blockId + " in " + (System.currentTimeMillis - time)  + " ms")
    reportPushedBlock(blockId, -1, optionalMetadata)
  }

  /** Push a block of received data as bytes into the block generator. */
  def pushBytes(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ) {
    val blockId = optionalBlockId.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    blockManager.putBytes(blockId, bytes, storageLevel, tellMaster = true)
    logDebug("Pushed block " + blockId + " in " + (System.currentTimeMillis - time)  + " ms")
    reportPushedBlock(blockId, -1, optionalMetadata)
  }

  /** Report pushed block */
  def reportPushedBlock(blockId: StreamBlockId, numRecords: Long, optionalMetadata: Option[Any]) {
    val blockInfo = ReceivedBlockInfo(receiverId, blockId, numRecords, optionalMetadata.orNull)
    trackerActor ! AddBlock(blockInfo)
    logDebug("Reported block " + blockId)
  }

  /** Add exceptions to a list */
  def reportError(message: String, throwable: Throwable) {
    exceptions += new Exception(message, throwable)
  }

  override def onReceiverStart() {
    blockGenerator.start()
    super.onReceiverStart()
  }

  override def onReceiverStop() {
    super.onReceiverStop()
    blockGenerator.stop()
    reportStop()
  }

  /** Report to the NetworkInputTracker that the receiver has stopped */
  private def reportStop() {
    val message = if (exceptions.isEmpty) {
      null
    } else if (exceptions.size == 1) {
      val e = exceptions.head
      "Exception in receiver " + receiverId + ": " + e.getMessage + "\n" + e.getStackTraceString
    } else {
      "Multiple exceptions in receiver " + receiverId + "(" + exceptions.size + "):\n"
      exceptions.zipWithIndex.map {
        case (e, i) => "Exception " + i + ": " + e.getMessage + "\n" + e.getStackTraceString
      }.mkString("\n")
    }
    logInfo("Deregistering receiver " + receiverId)
    val future = trackerActor.ask(DeregisterReceiver(receiverId, message))(askTimeout)
    Await.result(future, askTimeout)
    logInfo("Deregistered receiver " + receiverId)
    env.actorSystem.stop(actor)
    logInfo("Stopped receiver " + receiverId)
  }

  /** Generate new block ID */
  private def nextBlockId = StreamBlockId(receiverId, newBlockId.getAndIncrement)
}
