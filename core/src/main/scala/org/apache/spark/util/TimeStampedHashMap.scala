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

import org.apache.spark.Logging

private[util] case class TimeStampedValue[T](timestamp: Long, value: T)

/**
 * A map that stores the timestamp of when a key was inserted along with the value. If specified,
 * the timestamp of each pair can be updated every time it is accessed.
 * Key-value pairs whose timestamps are older than a particular
 * threshold time can then be removed using the clearOldValues method. It exposes a
 * scala.collection.mutable.Map interface to allow it to be a drop-in replacement for Scala
 * HashMaps.
 *
 * Internally, it uses a Java ConcurrentHashMap, so all operations on this HashMap are thread-safe.
 *
 * @param updateTimeStampOnGet When enabled, the timestamp of a pair will be
 *                             updated when it is accessed
 */
private[spark] class TimeStampedHashMap[A, B](updateTimeStampOnGet: Boolean = false)
  extends WrappedJavaHashMap[A, B, A, TimeStampedValue[B]] with Logging {

  protected[util] val internalJavaMap = new ConcurrentHashMap[A, TimeStampedValue[B]]()

  protected[util] def newInstance[K1, V1](): WrappedJavaHashMap[K1, V1, _, _] = {
    new TimeStampedHashMap[K1, V1]()
  }

  def internalMap = internalJavaMap

  override def get(key: A): Option[B] = {
    val timeStampedValue = internalMap.get(key)
    if (updateTimeStampOnGet && timeStampedValue != null) {
      internalJavaMap.replace(key, timeStampedValue, TimeStampedValue(currentTime, timeStampedValue.value))
    }
    Option(timeStampedValue).map(_.value)
  }
  @inline override protected def externalValueToInternalValue(v: B): TimeStampedValue[B] = {
    new TimeStampedValue(currentTime, v)
  }

  @inline override protected def internalValueToExternalValue(iv: TimeStampedValue[B]): B = {
    iv.value
  }

  /** Atomically put if a key is absent. This exposes the existing API of ConcurrentHashMap. */
  def putIfAbsent(key: A, value: B): Option[B] = {
    val prev = internalJavaMap.putIfAbsent(key, TimeStampedValue(currentTime, value))
    Option(prev).map(_.value)
  }

  /**
   * Removes old key-value pairs that have timestamp earlier than `threshTime`,
   * calling the supplied function on each such entry before removing.
   */
  def clearOldValues(threshTime: Long, f: (A, B) => Unit) {
    val iterator = internalJavaMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getValue.timestamp < threshTime) {
        f(entry.getKey, entry.getValue.value)
        logDebug("Removing key " + entry.getKey)
        iterator.remove()
      }
    }
  }

  /**
   * Removes old key-value pairs that have timestamp earlier than `threshTime`
   */
  def clearOldValues(threshTime: Long) {
    clearOldValues(threshTime, (_, _) => ())
  }

  private def currentTime: Long = System.currentTimeMillis()
}
