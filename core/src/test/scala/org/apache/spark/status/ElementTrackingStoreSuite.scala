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

import org.mockito.Mockito._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.kvstore._

class ElementTrackingStoreSuite extends SparkFunSuite {

  import config._

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
