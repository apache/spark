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
 * Builds a map in which keys are case insensitive
 */
class CaseInsensitiveMap(map: Map[String, String]) extends Map[String, String]
  with Serializable {

  private val caseSensitiveKeyMap = map.map { kv => kv._1.toLowerCase -> kv._1 }

  val baseMap = map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

  override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

  override def contains(k: String): Boolean = baseMap.contains(k.toLowerCase)

  override def +[B1 >: String](kv: (String, B1)): Map[String, B1] = {
    new CaseInsensitiveMap(baseMap.map(bkv => bkv.copy(_1 = caseSensitiveKeyMap.get(bkv._1).get))
      + kv.copy(_2 = kv._2.asInstanceOf[String]))
  }

  override def iterator: Iterator[(String, String)] = baseMap.iterator

  override def -(key: String): Map[String, String] = {
    new CaseInsensitiveMap((baseMap - key.toLowerCase)
      .map(kv => kv.copy(_1 = caseSensitiveKeyMap.get(kv._1).get)))
  }

  /**
   * Returns the case-sensitive key that the case-insensitive maps was based on.
   * Used in cases where original user input key is required to pass it to external
   * data sources that may be case-sensitive like some JDBC data sources.
   */
  def getCaseSensitiveKey(k : String) : Option[String] = caseSensitiveKeyMap.get(k.toLowerCase())
}
