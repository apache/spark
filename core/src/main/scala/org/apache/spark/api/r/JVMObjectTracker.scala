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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

/** JVM object ID wrapper */
private[r] case class JVMObjectId(id: String)

/**
 * Counter that tracks JVM objects returned to R.
 * This is useful for referencing these objects in RPC calls.
 */
private[r] class JVMObjectTracker {

  private[this] val objMap = new ConcurrentHashMap[JVMObjectId, Object]()
  private[this] val objCounter = new AtomicInteger()

  /**
   * Returns the JVM object associated with the input key.
   * Returns null if the key does not exist or has been removed.
   */
  @throws[NullPointerException]("if key is null")
  final def get(id: JVMObjectId): Object = objMap.get(id)

  /**
   * Adds a JVM object to track and returns assigned ID, which is unique within this tracker.
   */
  final def addAndGetId(obj: Object): JVMObjectId = {
    val id = JVMObjectId(objCounter.getAndIncrement().toString)
    objMap.put(id, obj)
    id
  }

  /**
   * Removes a JVM object with the specific ID from the tracker.
   */
  final def remove(id: JVMObjectId): Object = objMap.remove(id)

  /**
   * Number of JVM objects being tracked.
   */
  final def size: Int = objMap.size()

  /**
   * Clears the tracker.
   */
  final def clear(): Unit = objMap.clear()
}
