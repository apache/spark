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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.{HashMap, Iterator}
import java.util.Map.Entry
import java.util.function.Function

/**
 * The [[KeyTransformingMap]] is a partial implementation of [[mutable.Map]] that transforms input
 * keys with a custom [[mapKey]] method.
 */
private abstract class KeyTransformingMap[K, V] {
  private val impl = new HashMap[K, V]

  def get(key: K): Option[V] = Option(impl.get(mapKey(key)))

  def put(key: K, value: V): V = impl.put(mapKey(key), value)

  def contains(key: K): Boolean = impl.containsKey(mapKey(key))

  def computeIfAbsent(key: K, compute: Function[K, V]): V =
    impl.computeIfAbsent(mapKey(key), compute)

  def iterator: Iterator[Entry[K, V]] = impl.entrySet().iterator()

  def +=(kv: (K, V)): this.type = {
    impl.put(mapKey(kv._1), kv._2)
    this
  }

  def -=(key: K): this.type = {
    impl.remove(mapKey(key))
    this
  }

  def mapKey(key: K): K
}
