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
import scala.collection.mutable.Set

private[spark] class TimeStampedHashSet[A] extends Set[A] {
  val internalMap = new ConcurrentHashMap[A, Long]()

  def contains(key: A): Boolean = {
    internalMap.contains(key)
  }

  def iterator: Iterator[A] = {
    val jIterator = internalMap.entrySet().iterator()
    JavaConversions.asScalaIterator(jIterator).map(_.getKey)
  }

  override def + (elem: A): Set[A] = {
    val newSet = new TimeStampedHashSet[A]
    newSet ++= this
    newSet += elem
    newSet
  }

  override def - (elem: A): Set[A] = {
    val newSet = new TimeStampedHashSet[A]
    newSet ++= this
    newSet -= elem
    newSet
  }

  override def += (key: A): this.type = {
    internalMap.put(key, currentTime)
    this
  }

  override def -= (key: A): this.type = {
    internalMap.remove(key)
    this
  }

  override def empty: Set[A] = new TimeStampedHashSet[A]()

  override def size(): Int = internalMap.size()

  override def foreach[U](f: (A) => U): Unit = {
    val iterator = internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      f(iterator.next.getKey)
    }
  }

  /**
   * Removes old values that have timestamp earlier than `threshTime`
   */
  def clearOldValues(threshTime: Long) {
    val iterator = internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getValue < threshTime) {
        iterator.remove()
      }
    }
  }

  private def currentTime: Long = System.currentTimeMillis()
}
