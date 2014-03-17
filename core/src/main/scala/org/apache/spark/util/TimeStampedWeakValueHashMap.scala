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

import scala.collection.{JavaConversions, immutable}

import java.util
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.Logging
import java.util.concurrent.atomic.AtomicInteger

private[util] case class TimeStampedWeakValue[T](timestamp: Long, weakValue: WeakReference[T]) {
  def this(timestamp: Long, value: T) = this(timestamp, new WeakReference[T](value))
}

/**
 * A map that stores the timestamp of when a key was inserted along with the value,
 * while ensuring that the values are weakly referenced. If the value is garbage collected and
 * the weak reference is null, get() operation returns the key be non-existent. However,
 * the key is actually not removed in the current implementation. Key-value pairs whose
 * timestamps are older than a particular threshold time can then be removed using the
 * clearOldValues method. It exposes a scala.collection.mutable.Map interface to allow it to be a
 * drop-in replacement for Scala HashMaps.
 *
 * Internally, it uses a Java ConcurrentHashMap, so all operations on this HashMap are thread-safe.
 */

private[spark] class TimeStampedWeakValueHashMap[A, B]()
  extends WrappedJavaHashMap[A, B, A, TimeStampedWeakValue[B]] with Logging {

  /** Number of inserts after which keys whose weak ref values are null will be cleaned */
  private val CLEANUP_INTERVAL = 1000

  /** Counter for counting the number of inserts */
  private val insertCounts = new AtomicInteger(0)

  protected[util] val internalJavaMap: util.Map[A, TimeStampedWeakValue[B]] = {
    new ConcurrentHashMap[A, TimeStampedWeakValue[B]]()
  }

  protected[util] def newInstance[K1, V1](): WrappedJavaHashMap[K1, V1, _, _] = {
    new TimeStampedWeakValueHashMap[K1, V1]()
  }

  override def +=(kv: (A, B)): this.type = {
    // Cleanup null value at certain intervals
    if (insertCounts.incrementAndGet() % CLEANUP_INTERVAL == 0) {
      cleanNullValues()
    }
    super.+=(kv)
  }

  override def get(key: A): Option[B] = {
    Option(internalJavaMap.get(key)) match {
      case Some(weakValue) =>
        val value = weakValue.weakValue.get
        if (value == null) {
          internalJavaMap.remove(key)
        }
        Option(value)
      case None =>
        None
    }
  }

  @inline override protected def externalValueToInternalValue(v: B): TimeStampedWeakValue[B] = {
    new TimeStampedWeakValue(currentTime, v)
  }

  @inline override protected def internalValueToExternalValue(iv: TimeStampedWeakValue[B]): B = {
    iv.weakValue.get
  }

  override def iterator: Iterator[(A, B)] = {
    val iterator = internalJavaMap.entrySet().iterator()
    JavaConversions.asScalaIterator(iterator).flatMap(kv => {
      val (key, value) = (kv.getKey, kv.getValue.weakValue.get)
      if (value != null) Seq((key, value)) else Seq.empty
    })
  }

  /**
   * Removes old key-value pairs that have timestamp earlier than `threshTime`,
   * calling the supplied function on each such entry before removing.
   */
  def clearOldValues(threshTime: Long, f: (A, B) => Unit = null) {
    val iterator = internalJavaMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getValue.timestamp < threshTime) {
        val value = entry.getValue.weakValue.get
        if (f != null && value != null) {
          f(entry.getKey, value)
        }
        logDebug("Removing key " + entry.getKey)
        iterator.remove()
      }
    }
  }

  /**
   * Removes keys whose weak referenced values have become null.
   */
  private def cleanNullValues() {
    val iterator = internalJavaMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getValue.weakValue.get == null) {
        logDebug("Removing key " + entry.getKey)
        iterator.remove()
      }
    }
  }

  private def currentTime = System.currentTimeMillis()
}
