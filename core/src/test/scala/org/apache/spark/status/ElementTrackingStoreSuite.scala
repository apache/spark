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

package org.apache.spark.status

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Status._
import org.apache.spark.status.ElementTrackingStore._
import org.apache.spark.util.kvstore._

class ElementTrackingStoreSuite extends SparkFunSuite with Eventually {

  test("asynchronous tracking single-fire") {
    val store = mock(classOf[KVStore])
    val tracking = new ElementTrackingStore(store, new SparkConf()
      .set(ASYNC_TRACKING_ENABLED, true))

    val done = new AtomicBoolean(false)
    val type1 = new AtomicInteger(0)
    var queued0: WriteQueueResult = null
    var queued1: WriteQueueResult = null
    var queued2: WriteQueueResult = null
    var queued3: WriteQueueResult = null

    tracking.addTrigger(classOf[Type1], 1) { count =>
      val count = type1.getAndIncrement()

      count match {
        case 0 =>
          // while in the asynchronous thread, attempt to increment twice.  The first should
          // succeed, the second should be skipped
          queued1 = tracking.write(new Type1, checkTriggers = true)
          queued2 = tracking.write(new Type1, checkTriggers = true)
        case 1 =>
          // Verify that once we've started deliver again, that we can enqueue another
          queued3 = tracking.write(new Type1, checkTriggers = true)
        case 2 =>
          done.set(true)
      }
    }

    when(store.count(classOf[Type1])).thenReturn(2L)
    queued0 = tracking.write(new Type1, checkTriggers = true)
    eventually {
      done.get() shouldEqual true
    }

    tracking.close(false)
    assert(queued0 == WriteQueued)
    assert(queued1 == WriteQueued)
    assert(queued2 == WriteSkippedQueue)
    assert(queued3 == WriteQueued)
  }

  test("tracking for multiple types") {
    val store = mock(classOf[KVStore])
    val tracking = new ElementTrackingStore(store, new SparkConf()
      .set(ASYNC_TRACKING_ENABLED, false))

    var type1 = 0L
    var type2 = 0L
    var flushed = false

    tracking.addTrigger(classOf[Type1], 100) { count =>
      type1 = count
    }
    tracking.addTrigger(classOf[Type2], 1000) { count =>
      type2 = count
    }
    tracking.onFlush {
      flushed = true
    }

    when(store.count(classOf[Type1])).thenReturn(1L)
    tracking.write(new Type1, true)
    assert(type1 === 0L)
    assert(type2 === 0L)

    when(store.count(classOf[Type1])).thenReturn(100L)
    tracking.write(new Type1, true)
    assert(type1 === 0L)
    assert(type2 === 0L)

    when(store.count(classOf[Type1])).thenReturn(101L)
    tracking.write(new Type1, true)
    assert(type1 === 101L)
    assert(type2 === 0L)

    when(store.count(classOf[Type1])).thenReturn(200L)
    tracking.write(new Type1, true)
    assert(type1 === 200L)
    assert(type2 === 0L)

    when(store.count(classOf[Type2])).thenReturn(500L)
    tracking.write(new Type2, true)
    assert(type1 === 200L)
    assert(type2 === 0L)

    when(store.count(classOf[Type2])).thenReturn(1000L)
    tracking.write(new Type2, true)
    assert(type1 === 200L)
    assert(type2 === 0L)

    when(store.count(classOf[Type2])).thenReturn(2000L)
    tracking.write(new Type2, true)
    assert(type1 === 200L)
    assert(type2 === 2000L)

    tracking.close(false)
    assert(flushed)
    verify(store, never()).close()
  }

  private class Type1
  private class Type2

}
