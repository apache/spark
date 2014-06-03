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

package org.apache.spark.sql

import scala.collection.mutable

/**
 * SQLConf holds potentially query-dependent, mutable config parameters and hints.
 */
class SQLConf {

  private val settings = new mutable.HashMap[String, String]()

  def set(key: String, value: String): SQLConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value
    this
  }

  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  def get(key: String, defaultValue: String): String = {
    settings.getOrElse(key, defaultValue)
  }

  def getAll: Array[(String, String)] = settings.clone().toArray

  def getOption(key: String): Option[String] = {
    settings.get(key)
  }

  def contains(key: String): Boolean = settings.contains(key)

  def toDebugString: String = {
    settings.toArray.sorted.map{ case (k, v) => s"$k=$v" }.mkString("\n")
  }

}
