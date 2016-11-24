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

package org.apache.spark

import java.util.UUID
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Utility wrapper to "smuggle" objects into tasks while bypassing serialization.
 * This is intended for testing purposes, primarily to make locks, semaphores, and
 * other constructs that would not survive serialization available from within tasks.
 * A Smuggle reference is itself serializable, but after being serialized and
 * deserialized, it still refers to the same underlying "smuggled" object, as long
 * as it was deserialized within the same JVM. This can be useful for tests that
 * depend on the timing of task completion to be deterministic, since one can "smuggle"
 * a lock or semaphore into the task, and then the task can block until the test gives
 * the go-ahead to proceed via the lock.
 */
class Smuggle[T] private(val key: Symbol) extends Serializable {
  def smuggledObject: T = Smuggle.get(key)
}


object Smuggle {
  /**
   * Wraps the specified object to be smuggled into a serialized task without
   * being serialized itself.
   *
   * @param smuggledObject
   * @tparam T
   * @return Smuggle wrapper around smuggledObject.
   */
  def apply[T](smuggledObject: T): Smuggle[T] = {
    val key = Symbol(UUID.randomUUID().toString)
    lock.writeLock().lock()
    try {
      smuggledObjects += key -> smuggledObject
    } finally {
      lock.writeLock().unlock()
    }
    new Smuggle(key)
  }

  private val lock = new ReentrantReadWriteLock
  private val smuggledObjects = mutable.WeakHashMap.empty[Symbol, Any]

  private def get[T](key: Symbol) : T = {
    lock.readLock().lock()
    try {
      smuggledObjects(key).asInstanceOf[T]
    } finally {
      lock.readLock().unlock()
    }
  }

  /**
   * Implicit conversion of a Smuggle wrapper to the object being smuggled.
   *
   * @param smuggle the wrapper to unpack.
   * @tparam T
   * @return the smuggled object represented by the wrapper.
   */
  implicit def unpackSmuggledObject[T](smuggle : Smuggle[T]): T = smuggle.smuggledObject

}
