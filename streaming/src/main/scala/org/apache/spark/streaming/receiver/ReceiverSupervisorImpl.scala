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
import scala.concurrent.{Promise, Future, Await}

import akka.actor.{Actor, Props}
import akka.pattern.ask

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.scheduler.DeregisterReceiver
import org.apache.spark.streaming.scheduler.AddBlock
import org.apache.spark.streaming.scheduler.RegisterReceiver
import com.google.common.base.Throwables

import scala.util.Try

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
    val url = "akka.tcp://spark@%s:%s/user/ReceiverTracker".format(ip, port)
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
    pushData(arrayBuffer, optionalMetadata, optionalBlockId)
  }

  /** Store a iterator of received data as a data block into Spark's memory. */
  def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ) {
    pushData(iterator, optionalMetadata, optionalBlockId)
  }

  /** Store the bytes of received data as a data block into Spark's memory. */
  def pushBytes(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ) {
    pushData(bytes, optionalMetadata, optionalBlockId)
  }

  /**
   * Store an ArrayBuffer of received data as a data block into Spark's memory. The future can
   * return the result of the attempt to store reliably.
   */
  override def pushArrayBufferReliably(
    arrayBuffer: ArrayBuffer[_],
    optionalMetadata: Option[Any],
    optionalBlockId: Option[StreamBlockId]
    ): Future[StoreResult] = {
    pushDataReliably(arrayBuffer, optionalMetadata, optionalBlockId)
  }

  /**
   * Store a iterator of received data as a data block into Spark's memory. The future can return
   * the result of the attempt to store reliably.
   */
  override def pushIteratorReliably(
    iterator: Iterator[_],
    optionalMetadata: Option[Any],
    optionalBlockId: Option[StreamBlockId]
    ): Future[StoreResult] = {
    pushDataReliably(iterator, optionalMetadata, optionalBlockId)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory. The future can return
   * the result of the attempt to store reliably.
   */
  override def pushBytesReliably(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Future[StoreResult] = {
    pushDataReliably(bytes, optionalMetadata, optionalBlockId)
  }

  /**
   * Store the data reliably as a data block into Spark's memory. The data can be a
   * [[ByteBuffer]], [[ArrayBuffer]] or an [[Iterator]]. Once the data is pushed,
   * this method stores the data reliably but asynchronously. The result of the attempt to
   * successfully store the data can be retrieved from the future returned from this method.
   */
  private def pushDataReliably(
      data: Any,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Future[StoreResult] = {
    val blockId = pushData(data, optionalMetadata, optionalBlockId)
    completePushReliably(blockId)
  }

  /**
   * Store the data reliably as a data block into Spark's memory. The data can be a
   * [[ByteBuffer]], [[ArrayBuffer]] or an [[Iterator]].
   */
  private def pushData(
      data: Any,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): StreamBlockId = {
    val blockId = optionalBlockId.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    data match {
      case bytes: ByteBuffer =>
        blockManager.putBytes(blockId, bytes, storageLevel, tellMaster = true)
      case arrayBuffer: ArrayBuffer[_] =>
        blockManager.putArray(blockId, arrayBuffer.toArray[Any], storageLevel, tellMaster = true)
      case iterator: Iterator[_] =>
        blockManager.putIterator(blockId, iterator, storageLevel, tellMaster = true)
      case _ => throw new RuntimeException("Unknown Data Type!")
    }
    logDebug("Pushed block " + blockId + " in " + (System.currentTimeMillis - time)  + " ms")
    reportPushedBlock(blockId, -1, optionalMetadata)
    blockId
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

  private def completePushReliably(blockId: StreamBlockId): Future[StoreResult] = {
    val successPromise = Promise[StoreResult]()
    // Right now, this method is a no-op, but once we have BlockManagerMaster replicated/stored
    // to some storage system, we'd have to asynchronously instruct the BMM to persist the
    // added block, wait for a response and mark success only when that is successfully done.
    successPromise.success(new StoreResult(true, None))
    successPromise.future
  }

  /** Generate new block ID */
  private def nextBlockId = StreamBlockId(streamId, newBlockId.getAndIncrement)

}
