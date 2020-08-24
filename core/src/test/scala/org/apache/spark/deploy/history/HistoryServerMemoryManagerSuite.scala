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

package org.apache.spark.deploy.history

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History._

class HistoryServerMemoryManagerSuite extends SparkFunSuite {

  private val MAX_USAGE = 3L

  test("lease and release memory") {
    val conf = new SparkConf().set(MAX_IN_MEMORY_STORE_USAGE, MAX_USAGE)
    val manager = new HistoryServerMemoryManager(conf)

    // Memory usage estimation for non-compressed log file is filesize / 2
    manager.lease("app1", None, 2L, None)
    manager.lease("app2", None, 2L, None)
    manager.lease("app3", None, 2L, None)
    assert(manager.currentUsage.get === 3L)
    assert(manager.active.size === 3)
    assert(manager.active.get(("app1", None)) === Some(1L))

    intercept[RuntimeException] {
      manager.lease("app4", None, 2L, None)
    }

    // Releasing a non-existent app is a no-op
    manager.release("app4", None)
    assert(manager.currentUsage.get === 3L)

    manager.release("app1", None)
    assert(manager.currentUsage.get === 2L)
    assert(manager.active.size === 2)

    manager.lease("app4", None, 2L, None)
    assert(manager.currentUsage.get === 3L)
    assert(manager.active.size === 3)
  }
}
