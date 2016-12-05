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

package org.apache.spark.api.r

import org.apache.spark.SparkFunSuite

class JVMObjectTrackerSuite extends SparkFunSuite {
  test("JVMObjectTracker") {
    val tracker = new JVMObjectTracker
    assert(tracker.size === 0)
    withClue("an empty tracker can be cleared") {
      tracker.clear()
    }
    intercept[NullPointerException] {
      tracker.get(null)
    }

    val obj1 = new Object
    val id1 = tracker.addAndGetId(obj1)
    assert(id1 != null && id1.id != null)
    assert(tracker.size === 1)
    assert(tracker.get(id1).eq(obj1))

    val obj2 = new Object
    val id2 = tracker.addAndGetId(obj2)
    assert(id2 != null && id2.id != null)
    assert(id1 !== id2)
    assert(tracker.size === 2)
    assert(tracker.get(id2).eq(obj2))

    tracker.remove(id1)
    assert(tracker.size === 1)
    assert(tracker.get(id1) === null)
    assert(tracker.get(id2).eq(obj2))

    val obj3 = new Object
    val id3 = tracker.addAndGetId(obj3)
    assert(tracker.size === 2)
    assert(id3 != id1)
    assert(id3 != id2)
    assert(tracker.get(id3).eq(obj3))

    tracker.clear()
    assert(tracker.size === 0)
    assert(tracker.get(id1) === null)
    assert(tracker.get(id2) === null)
    assert(tracker.get(id3) === null)
  }
}
