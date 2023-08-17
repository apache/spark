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

package org.apache.spark.sql.connect.client

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging


private[sql] class Heartbeat {
  def beat[T](fn: => T): T = fn

  def beatIterator[T](fn: => CloseableIterator[T]): CloseableIterator[T] = fn

  def heartbeatLevel: Int = 0

  def pingCount: Long = 0
}

/**
 * This class implements Heartbeat.
 * Heartbeat consists of sending a dummy request once in a while if there is at least
 * one long-term query running.
 *
 * Otherwise the connection may disconnect before client receives the response back.
 */
private[sql] class HeartbeatImpl(bstub: CustomSparkConnectBlockingStub)
  extends Heartbeat with Logging {
  val beatLevel: AtomicInteger = new AtomicInteger(0)
  var count: AtomicLong = new AtomicLong(0)

  override def heartbeatLevel: Int = beatLevel.get()

  override def pingCount: Long = count.get()

  private val pingThread = new Thread() {
    override def run(): Unit = {
      while (!bstub.channel.isTerminated) {
        if (beatLevel.get() > 0) {
          logDebug("Executing ping")
          try {
            count.incrementAndGet()
            bstub.executePing()
          } catch {
            case NonFatal(e) => logDebug(s"Caught exception ${e} during ping")
          }
        }

        Thread.sleep(Heartbeat.SLEEP_TIMER_MILLIS)
      }
    }
  }
  pingThread.setDaemon(true)
  pingThread.start()

  override def beat[T](fn: => T): T = {
    beatLevel.incrementAndGet()
    try {
      fn
    } finally {
      beatLevel.decrementAndGet()
    }
  }

  override def beatIterator[T](fn: => CloseableIterator[T]): CloseableIterator[T] = {
    beat(convertIterator(fn))
  }

  def convertIterator[T](iterator: CloseableIterator[T]): CloseableIterator[T] = {
    new CloseableIterator[T] {
      beatLevel.incrementAndGet()
      var isClosed = false

      def close(errored: Boolean) = {
        if (!isClosed) {
          isClosed = true
          if (!errored) {
            // don't call underlying iterator in case previous operation with it
            // failed with an exception
            iterator.close()
          }
          beatLevel.decrementAndGet()
        }
      }

      override def close(): Unit = {
        close(false)
      }

      override def hasNext: Boolean = {
        // Treat fully consumed iterator as also closed
        // This allows to skip explicit iterator closing in functions
        // which completely consume iterators they get from execute()

        try {
          if (isClosed || !iterator.hasNext) {
            close()
            return false
          }
          true
        } catch {
          case NonFatal(e) =>
            close(true)
            throw e
        }
      }

      override def next(): T = {
        if (isClosed) {
          throw new NoSuchElementException()
        }

        try {
          iterator.next
        } catch {
          case _: NoSuchElementException =>
            // iterator completely consumed, close it.
            close()
            throw new NoSuchElementException()
          case NonFatal(e) =>
            // There is error down the line. Close it too
            close(true)
            throw e
        }
      }
    }
  }
}

object Heartbeat {
  private[sql] var SLEEP_TIMER_MILLIS = 120 * 1000
}
