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

package org.apache.spark.sql.catalyst.util

/**
 * Build a map with String type of key, and it also supports either key case
 * sensitive or insensitive.
 */
object StringKeyHashMap {
  def apply[T](caseSensitive: Boolean): StringKeyHashMap[T] = if (caseSensitive) {
    new StringKeyHashMap[T](identity)
  } else {
    new StringKeyHashMap[T](_.toLowerCase)
  }

}


class StringKeyHashMap[T](normalizer: (String) => String) {
  private val base = new collection.mutable.HashMap[String, T]()

  def apply(key: String): T = base(normalizer(key))

  def get(key: String): Option[T] = base.get(normalizer(key))

  def put(key: String, value: T): Option[T] = base.put(normalizer(key), value)

  def remove(key: String): Option[T] = base.remove(normalizer(key))

  def iterator: Iterator[(String, T)] = base.toIterator

  def clear(): Unit = base.clear()
}
