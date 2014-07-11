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

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.util.{RecurringTimer, SystemClock}

/** Listener object for BlockGenerator events */
private[streaming] trait BlockGeneratorListener {
  /** Called when a new block needs to be pushed */
  def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_])
  /** Called when an error has occurred in BlockGenerator */
  def onError(message: String, throwable: Throwable)
}

/**
 * Generates batches of objects received by a
 * [[org.apache.spark.streaming.receiver.Receiver]] and puts them into appropriately
 * named blocks at regular intervals. This class starts two threads,
 * one to periodically start a new batch and prepare the previous batch of as a block,
 * the other to push the blocks into the block manager.
 */
private[streaming] class BlockGenerator(
    listener: BlockGeneratorListener,
    receiverId: Int,
    conf: SparkConf
  ) extends RateLimiter(conf) with Logging {

  private case class Block(id: StreamBlockId, buffer: ArrayBuffer[Any])

  private val clock = new SystemClock()
  private val blockInterval = conf.getLong("spark.streaming.blockInterval", 200)
  private val blockIntervalTimer =
    new RecurringTimer(clock, blockInterval, updateCurrentBuffer, "BlockGenerator")
  private val blockQueueSize = conf.getInt("spark.streaming.blockQueueSize", 10)
  private val blocksForPushing = new ArrayBlockingQueue[Block](blockQueueSize)
  private val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

  @volatile private var currentBuffer = new ArrayBuffer[Any]
  @volatile private var stopped = false

  /** Start block generating and pushing threads. */
  def start() {
    blockIntervalTimer.start()
    blockPushingThread.start()
    logInfo("Started BlockGenerator")
  }

  /** Stop all threads. */
  def stop() {
    logInfo("Stopping BlockGenerator")
    blockIntervalTimer.stop(interruptTimer = false)
    stopped = true
    logInfo("Waiting for block pushing thread")
    blockPushingThread.join()
    logInfo("Stopped BlockGenerator")
  }

  /**
   * Push a single data item into the buffer. All received data items
   * will be periodically pushed into BlockManager.
   */
  def += (data: Any): Unit = synchronized {
    waitToPush()
    currentBuffer += data
  }

  /** Change the buffer to which single records are added to. */
  private def updateCurrentBuffer(time: Long): Unit = synchronized {
    try {
      val newBlockBuffer = currentBuffer
      currentBuffer = new ArrayBuffer[Any]
      if (newBlockBuffer.size > 0) {
        val blockId = StreamBlockId(receiverId, time - blockInterval)
        val newBlock = new Block(blockId, newBlockBuffer)
        blocksForPushing.put(newBlock)  // put is blocking when queue is full
        logDebug("Last element in " + blockId + " is " + newBlockBuffer.last)
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case t: Throwable =>
        reportError("Error in block updating thread", t)
    }
  }

  /** Keep pushing blocks to the BlockManager. */
  private def keepPushingBlocks() {
    logInfo("Started block pushing thread")
    try {
      while(!stopped) {
        Option(blocksForPushing.poll(100, TimeUnit.MILLISECONDS)) match {
          case Some(block) => pushBlock(block)
          case None =>
        }
      }
      // Push out the blocks that are still left
      logInfo("Pushing out the last " + blocksForPushing.size() + " blocks")
      while (!blocksForPushing.isEmpty) {
        logDebug("Getting block ")
        val block = blocksForPushing.take()
        pushBlock(block)
        logInfo("Blocks left to push " + blocksForPushing.size())
      }
      logInfo("Stopped block pushing thread")
    } catch {
      case ie: InterruptedException =>
        logInfo("Block pushing thread was interrupted")
      case t: Throwable =>
        reportError("Error in block pushing thread", t)
    }
  }

  private def reportError(message: String, t: Throwable) {
    logError(message, t)
    listener.onError(message, t)
  }
  
  private def pushBlock(block: Block) {
    listener.onPushBlock(block.id, block.buffer)
    logInfo("Pushed block " + block.id)
  }
}
