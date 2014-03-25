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

import scala.collection.mutable.{ArrayBuffer, SynchronizedMap}

import java.util.{Collections, LinkedHashMap}
import java.util.Map.{Entry => JMapEntry}
import scala.reflect.ClassTag

/**
 * A map that upper bounds the number of key-value pairs present in it. It can be configured to
 * drop the least recently user pair or the earliest inserted pair. It exposes a
 * scala.collection.mutable.Map interface to allow it to be a drop-in replacement for Scala
 * HashMaps.
 *
 * Internally, a Java LinkedHashMap is used to get insert-order or access-order behavior.
 * Note that the LinkedHashMap is not thread-safe and hence, it is wrapped in a
 * Collections.synchronizedMap. However, getting the Java HashMap's iterator and
 * using it can still lead to ConcurrentModificationExceptions. Hence, the iterator()
 * function is overridden to copy the all pairs into an ArrayBuffer and then return the
 * iterator to the ArrayBuffer. Also, the class apply the trait SynchronizedMap which
 * ensures that all calls to the Scala Map API are synchronized. This together ensures
 * that ConcurrentModificationException is never thrown.
 *
 * @param bound   max number of key-value pairs
 * @param useLRU  true = least recently used/accessed will be dropped when bound is reached,
 *                false = earliest inserted will be dropped
 */
private[spark] class BoundedHashMap[A, B](bound: Int, useLRU: Boolean)
  extends WrappedJavaHashMap[A, B, A, B] with SynchronizedMap[A, B] {

  private[util] val internalJavaMap = Collections.synchronizedMap(new LinkedHashMap[A, B](
    bound / 8, (0.75).toFloat, useLRU) {
    override protected def removeEldestEntry(eldest: JMapEntry[A, B]): Boolean = {
      size() > bound
    }
  })

  private[util] def newInstance[K1, V1](): WrappedJavaHashMap[K1, V1, _, _] = {
    new BoundedHashMap[K1, V1](bound, useLRU)
  }

  /**
   * Overriding iterator to make sure that the internal Java HashMap's iterator
   * is not concurrently modified. This can be a performance issue and this should be overridden
   * if it is known that this map will not be used in a multi-threaded environment.
   */
  override def iterator: Iterator[(A, B)] = {
    (new ArrayBuffer[(A, B)] ++= super.iterator).iterator
  }
}
