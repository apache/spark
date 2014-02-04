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
import scala.collection.JavaConversions
import scala.collection.mutable.Map
import scala.collection.immutable
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.Logging

/**
 * This is a custom implementation of scala.collection.mutable.Map which stores the insertion
 * timestamp along with each key-value pair. If specified, the timestamp of each pair can be
 * updated every time it is accessed. Key-value pairs whose timestamp are older than a particular
 * threshold time can then be removed using the clearOldValues method. This is intended to
 * be a drop-in replacement of scala.collection.mutable.HashMap.
 * @param updateTimeStampOnGet When enabled, the timestamp of a pair will be
 *                             updated when it is accessed
 */
class TimeStampedHashMap[A, B](updateTimeStampOnGet: Boolean = false)
  extends Map[A, B]() with Logging {
  val internalMap = new ConcurrentHashMap[A, (B, Long)]()

  def get(key: A): Option[B] = {
    val value = internalMap.get(key)
    if (value != null && updateTimeStampOnGet) {
      internalMap.replace(key, value, (value._1, currentTime))
    }
    Option(value).map(_._1)
  }

  def iterator: Iterator[(A, B)] = {
    val jIterator = internalMap.entrySet().iterator()
    JavaConversions.asScalaIterator(jIterator).map(kv => (kv.getKey, kv.getValue._1))
  }

  override def + [B1 >: B](kv: (A, B1)): Map[A, B1] = {
    val newMap = new TimeStampedHashMap[A, B1]
    newMap.internalMap.putAll(this.internalMap)
    newMap.internalMap.put(kv._1, (kv._2, currentTime))
    newMap
  }

  override def - (key: A): Map[A, B] = {
    val newMap = new TimeStampedHashMap[A, B]
    newMap.internalMap.putAll(this.internalMap)
    newMap.internalMap.remove(key)
    newMap
  }

  override def += (kv: (A, B)): this.type = {
    internalMap.put(kv._1, (kv._2, currentTime))
    this
  }

  // Should we return previous value directly or as Option ?
  def putIfAbsent(key: A, value: B): Option[B] = {
    val prev = internalMap.putIfAbsent(key, (value, currentTime))
    if (prev != null) Some(prev._1) else None
  }


  override def -= (key: A): this.type = {
    internalMap.remove(key)
    this
  }

  override def update(key: A, value: B) {
    this += ((key, value))
  }

  override def apply(key: A): B = {
    val value = internalMap.get(key)
    if (value == null) throw new NoSuchElementException()
    value._1
  }

  override def filter(p: ((A, B)) => Boolean): Map[A, B] = {
    JavaConversions.mapAsScalaConcurrentMap(internalMap).map(kv => (kv._1, kv._2._1)).filter(p)
  }

  override def empty: Map[A, B] = new TimeStampedHashMap[A, B]()

  override def size: Int = internalMap.size

  override def foreach[U](f: ((A, B)) => U) {
    val iterator = internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      val entry = iterator.next()
      val kv = (entry.getKey, entry.getValue._1)
      f(kv)
    }
  }

  def toMap: immutable.Map[A, B] = iterator.toMap

  /**
   * Removes old key-value pairs that have timestamp earlier than `threshTime`,
   * calling the supplied function on each such entry before removing.
   */
  def clearOldValues(threshTime: Long, f: (A, B) => Unit) {
    val iterator = internalMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getValue._2 < threshTime) {
        f(entry.getKey, entry.getValue._1)
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
