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

package org.apache.spark.storage

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{ReentrantLock, Condition}

import scala.collection.mutable

private[storage] class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {
  //Those lock conditions is used for PUT/GET/DROP thread to wait this block be ready
  private val lock = new ReentrantLock()
  private val putCondition = lock.newCondition()
  private val othersCondition = lock.newCondition()

  private val waitTypes = new BlockWaitCondition(Map("PUT" -> putCondition,
                                             "DROP" -> othersCondition,
                                             "GET" -> othersCondition))
  private var removed = false

  // To save space, 'pending' and 'failed' are encoded as special sizes:
  @volatile var size: Long = BlockInfo.BLOCK_PENDING
  private def pending: Boolean = size == BlockInfo.BLOCK_PENDING
  private def failed: Boolean = size == BlockInfo.BLOCK_FAILED
  private def initThread: Thread = BlockInfo.blockInfoInitThreads.get(this)

  setInitThread()

  private def setInitThread() {
    /* Set current thread as init thread - waitForReady will not block this thread
     * (in case there is non trivial initialization which ends up calling waitForReady
     * as part of initialization itself) */
    BlockInfo.blockInfoInitThreads.put(this, Thread.currentThread())
  }

  /**
   * Wait for this BlockInfo to be marked as ready (i.e. block is finished writing).
   * Return true if the block is available, false otherwise.
   */
  def waitForReady(waitType: String): Boolean = {
    if (pending && initThread != Thread.currentThread()) {
      try {
        lock.lock()
        while (pending) {
          waitTypes.waitTypeValWithName(waitType).await()
        }
      } finally {
        lock.unlock()
      }
    }
    !failed
  }

  /** Mark this BlockInfo as ready (i.e. block is finished writing) */
  def markReady(sizeInBytes: Long) {
    require(sizeInBytes >= 0, s"sizeInBytes was negative: $sizeInBytes")
    assert(pending)
    size = sizeInBytes
    BlockInfo.blockInfoInitThreads.remove(this)
    try {
      lock.lock()
      waitTypes.myValues.filter(_.getCount > 0).foreach(_.signalSuccess())
    } finally {
      lock.unlock()
    }
  }

  /** Mark this BlockInfo as ready but failed */
  def markFailureOrWithRemove(): Boolean = {
    assert(pending)
    size = BlockInfo.BLOCK_FAILED
    BlockInfo.blockInfoInitThreads.remove(this)
    try {
      lock.lock()
      removed = waitTypes.PUT.getCount == 0
      waitTypes.myValues.filter(_.getCount > 0).foreach(_.signalFailure())
    } finally {
      lock.unlock()
    }
    removed
  }

  def resetStatus() {
    size = BlockInfo.BLOCK_PENDING
  }

  def isRemoved = removed
}

private object BlockInfo {
  /* initThread is logically a BlockInfo field, but we store it here because
   * it's only needed while this block is in the 'pending' state and we want
   * to minimize BlockInfo's memory footprint. */
  private val blockInfoInitThreads = new ConcurrentHashMap[BlockInfo, Thread]

  private val BLOCK_PENDING: Long = -1L
  private val BLOCK_FAILED: Long = -2L
}

/**
 * A class for GET/PUT/DROP threads to use as lock.condition to wait block be ready
 * All GET/DROP thread wait one time for PUT whether succeed or failed
 * PUT thread do one by one until one succeed
 */
private[storage] class BlockWaitCondition(conditions: Map[String, Condition]) extends Enumeration {
  private val actualValues: mutable.HashSet[WaitTypeVal] = new mutable.HashSet[WaitTypeVal]()

  class WaitTypeVal(protected val finalCondition: Condition) extends Val {
    @volatile protected var count = 0
    def getCount = count

    def signalFailure() {
      count = 0
      finalCondition.signalAll()
    }

    def await() {
      count += 1
      finalCondition.await()
    }

    def signalSuccess() {
      count = 0
      finalCondition.signalAll()
    }
  }

  //Put Threads that wait will execute one by one until one will be succeed
  val PUT = new WaitTypeVal(conditions("PUT")) {
    override def signalFailure() {
      count -= 1
      finalCondition.signal()
    }
  }

  //DROP and GET Thread just return if one PUT thread is failed
  val DROP = new WaitTypeVal(conditions("DROP"))
  val GET = new WaitTypeVal(conditions("GET"))

  def  myValues = {
    if (actualValues.isEmpty) {
      values.foreach(v => actualValues += v.asInstanceOf[WaitTypeVal])
    }
    actualValues
  }

  def waitTypeValWithName(name: String) = this.withName(name).asInstanceOf[WaitTypeVal]
}
