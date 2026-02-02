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

package org.apache.spark.util

import java.util.concurrent.ConcurrentHashMap

/**
 * A special locking mechanism to provide locking with a given key. By providing the same key
 * (identity is tested using the `equals` method), we ensure there is only one `func` running at
 * the same time.
 *
 * @tparam K the type of key to identify a lock. This type must implement `equals` and `hashCode`
 *           correctly as it will be the key type of an internal Map.
 */
private[spark] class KeyLock[K] {

  private val lockMap = new ConcurrentHashMap[K, AnyRef]()

  private def acquireLock(key: K): Unit = {
    while (true) {
      val lock = lockMap.putIfAbsent(key, new Object)
      if (lock == null) return
      lock.synchronized {
        while (lockMap.get(key) eq lock) {
          lock.wait()
        }
      }
    }
  }

  private def releaseLock(key: K): Unit = {
    val lock = lockMap.remove(key)
    lock.synchronized {
      lock.notifyAll()
    }
  }

  /**
   * Run `func` under a lock identified by the given key. Multiple calls with the same key
   * (identity is tested using the `equals` method) will be locked properly to ensure there is only
   * one `func` running at the same time.
   */
  def withLock[T](key: K)(func: => T): T = {
    if (key == null) {
      throw new NullPointerException("key must not be null")
    }
    acquireLock(key)
    try {
      func
    } finally {
      releaseLock(key)
    }
  }
}
