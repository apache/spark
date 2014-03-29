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

import java.lang.ref.WeakReference

import scala.collection.{immutable, mutable}

/**
 * A wrapper of TimeStampedHashMap that ensures the values are weakly referenced and timestamped.
 *
 * If the value is garbage collected and the weak reference is null, get() operation returns
 * a non-existent value. However, the corresponding key is actually not removed in the current
 * implementation. Key-value pairs whose timestamps are older than a particular threshold time
 * can then be removed using the clearOldValues method. It exposes a scala.collection.mutable.Map
 * interface to allow it to be a drop-in replacement for Scala HashMaps.
 *
 * Internally, it uses a Java ConcurrentHashMap, so all operations on this HashMap are thread-safe.
 *
 * @param updateTimeStampOnGet Whether timestamp of a pair will be updated when it is accessed.
 */
private[spark] class TimeStampedWeakValueHashMap[A, B](updateTimeStampOnGet: Boolean = false)
  extends mutable.Map[A, B]() {

  import TimeStampedWeakValueHashMap._

  private val internalMap = new TimeStampedHashMap[A, WeakReference[B]](updateTimeStampOnGet)

  def get(key: A): Option[B] = internalMap.get(key)

  def iterator: Iterator[(A, B)] = internalMap.iterator

  override def + [B1 >: B](kv: (A, B1)): mutable.Map[A, B1] = {
    val newMap = new TimeStampedWeakValueHashMap[A, B1]
    newMap.internalMap += kv
    newMap
  }

  override def - (key: A): mutable.Map[A, B] = {
    val newMap = new TimeStampedWeakValueHashMap[A, B]
    newMap.internalMap -= key
    newMap
  }

  override def += (kv: (A, B)): this.type = {
    internalMap += kv
    this
  }

  override def -= (key: A): this.type = {
    internalMap -= key
    this
  }

  override def update(key: A, value: B) = this += ((key, value))

  override def apply(key: A): B = internalMap.apply(key)

  override def filter(p: ((A, B)) => Boolean): mutable.Map[A, B] = internalMap.filter(p)

  override def empty: mutable.Map[A, B] = new TimeStampedWeakValueHashMap[A, B]()

  override def size: Int = internalMap.size

  override def foreach[U](f: ((A, B)) => U) = internalMap.foreach(f)

  def putIfAbsent(key: A, value: B): Option[B] = internalMap.putIfAbsent(key, value)

  def toMap: immutable.Map[A, B] = iterator.toMap

  /**
   * Remove old key-value pairs that have timestamp earlier than `threshTime`.
   */
  def clearOldValues(threshTime: Long) = internalMap.clearOldValues(threshTime)

}

/**
 * Helper methods for converting to and from WeakReferences.
 */
private[spark] object TimeStampedWeakValueHashMap {

  /* Implicit conversion methods to WeakReferences */

  implicit def toWeakReference[V](v: V): WeakReference[V] = new WeakReference[V](v)

  implicit def toWeakReferenceTuple[K, V](kv: (K, V)): (K, WeakReference[V]) = {
    kv match { case (k, v) => (k, toWeakReference(v)) }
  }

  implicit def toWeakReferenceFunction[K, V, R](p: ((K, V)) => R): ((K, WeakReference[V])) => R = {
    (kv: (K, WeakReference[V])) => p(kv)
  }

  /* Implicit conversion methods from WeakReferences */

  implicit def fromWeakReference[V](ref: WeakReference[V]): V = ref.get

  implicit def fromWeakReferenceOption[V](v: Option[WeakReference[V]]): Option[V] = {
    v.map(fromWeakReference)
  }

  implicit def fromWeakReferenceTuple[K, V](kv: (K, WeakReference[V])): (K, V) = {
    kv match { case (k, v) => (k, fromWeakReference(v)) }
  }

  implicit def fromWeakReferenceIterator[K, V](
      it: Iterator[(K, WeakReference[V])]): Iterator[(K, V)] = {
    it.map(fromWeakReferenceTuple)
  }

  implicit def fromWeakReferenceMap[K, V](
      map: mutable.Map[K, WeakReference[V]]) : mutable.Map[K, V] = {
    mutable.Map(map.mapValues(fromWeakReference).toSeq: _*)
  }

}
