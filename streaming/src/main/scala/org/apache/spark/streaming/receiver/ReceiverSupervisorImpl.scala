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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await

import akka.actor.{Actor, Props}
import akka.pattern.ask

import com.google.common.base.Throwables

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.scheduler.DeregisterReceiver
import org.apache.spark.streaming.scheduler.AddBlock
import org.apache.spark.streaming.scheduler.RegisterReceiver

/**
 * Concrete implementation of [[org.apache.spark.streaming.receiver.ReceiverSupervisor]]
 * which provides all the necessary functionality for handling the data received by
 * the receiver. Specifically, it creates a [[org.apache.spark.streaming.receiver.BlockGenerator]]
 * object that is used to divide the received data stream into blocks of data.
 */
private[streaming] class ReceiverSupervisorImpl(
    receiver: Receiver[_],
    env: SparkEnv
  ) extends ReceiverSupervisor(receiver, env.conf) with Logging {

  private val blockManager = env.blockManager

  private val storageLevel = receiver.storageLevel

  /** Remote Akka actor for the ReceiverTracker */
  private val trackerActor = {
    val ip = env.conf.get("spark.driver.host", "localhost")
    val port = env.conf.getInt("spark.driver.port", 7077)
    val url = "akka.tcp://%s@%s:%s/user/ReceiverTracker".format(
      SparkEnv.driverActorSystemName, ip, port)
    env.actorSystem.actorSelection(url)
  }

  /** Timeout for Akka actor messages */
  private val askTimeout = AkkaUtils.askTimeout(env.conf)

  /** Akka actor for receiving messages from the ReceiverTracker in the driver */
  private val actor = env.actorSystem.actorOf(
    Props(new Actor {
      override def preStart() {
        logInfo("Registered receiver " + streamId)
        val msg = RegisterReceiver(
          streamId, receiver.getClass.getSimpleName, Utils.localHostName(), self)
        val future = trackerActor.ask(msg)(askTimeout)
        Await.result(future, askTimeout)
      }

      override def receive() = {
        case StopReceiver =>
          logInfo("Received stop signal")
          stop("Stopped by driver", None)
      }

      def ref = self
    }), "Receiver-" + streamId + "-" + System.currentTimeMillis())

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
  }, streamId, env.conf)

  /** Push a single record of received data into block generator. */
  def pushSingle(data: Any) {
    blockGenerator += (data)
  }

  /** Store an ArrayBuffer of received data as a data block into Spark's memory. */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ) {
    val blockId = optionalBlockId.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    blockManager.putArray(blockId, arrayBuffer.toArray[Any], storageLevel, tellMaster = true)
    logDebug("Pushed block " + blockId + " in " + (System.currentTimeMillis - time)  + " ms")
    reportPushedBlock(blockId, arrayBuffer.size, optionalMetadata)
  }

  /** Store a iterator of received data as a data block into Spark's memory. */
  def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ) {
    val blockId = optionalBlockId.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    blockManager.putIterator(blockId, iterator, storageLevel, tellMaster = true)
    logDebug("Pushed block " + blockId + " in " + (System.currentTimeMillis - time)  + " ms")
    reportPushedBlock(blockId, -1, optionalMetadata)
  }

  /** Store the bytes of received data as a data block into Spark's memory. */
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
    val blockInfo = ReceivedBlockInfo(streamId, blockId, numRecords, optionalMetadata.orNull)
    trackerActor ! AddBlock(blockInfo)
    logDebug("Reported block " + blockId)
  }

  /** Report error to the receiver tracker */
  def reportError(message: String, error: Throwable) {
    val errorString = Option(error).map(Throwables.getStackTraceAsString).getOrElse("")
    trackerActor ! ReportError(streamId, message, errorString)
    logWarning("Reported error " + message + " - " + error)
  }

  override protected def onStart() {
    blockGenerator.start()
  }

  override protected def onStop(message: String, error: Option[Throwable]) {
    blockGenerator.stop()
    env.actorSystem.stop(actor)
  }

  override protected def onReceiverStart() {
    val msg = RegisterReceiver(
      streamId, receiver.getClass.getSimpleName, Utils.localHostName(), actor)
    val future = trackerActor.ask(msg)(askTimeout)
    Await.result(future, askTimeout)
  }

  override protected def onReceiverStop(message: String, error: Option[Throwable]) {
    logInfo("Deregistering receiver " + streamId)
    val errorString = error.map(Throwables.getStackTraceAsString).getOrElse("")
    val future = trackerActor.ask(
      DeregisterReceiver(streamId, message, errorString))(askTimeout)
    Await.result(future, askTimeout)
    logInfo("Stopped receiver " + streamId)
  }

  /** Generate new block ID */
  private def nextBlockId = StreamBlockId(streamId, newBlockId.getAndIncrement)
}
