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

import scala.collection.mutable

/**
 * The [[KeyTransformingMap]] is a partial implementation of [[mutable.Map]] that transforms input
 * keys with a custom [[mapKey]] method.
 */
private abstract class KeyTransformingMap[K, V] {
  private val impl = new mutable.HashMap[K, V]

  def get(key: K): Option[V] = impl.get(mapKey(key))

  def contains(key: K): Boolean = impl.contains(mapKey(key))

  def iterator: Iterator[(K, V)] = impl.iterator

  def +=(kv: (K, V)): this.type = {
    impl += (mapKey(kv._1) -> kv._2)
    this
  }

  def -=(key: K): this.type = {
    impl -= mapKey(key)
    this
  }

  def mapKey(key: K): K
}
