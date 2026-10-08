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

package org.apache.spark.sql.internal

import java.util
import java.util.function.{BiConsumer, BiFunction, Function => JFunction}

/**
 * A thread-safe map of [[SQLConf]] settings that counts the mutating calls on it, so values
 * derived from its entries can be reused while [[modificationCount]] is unchanged.
 *
 * Like the `java.util.Collections.synchronizedMap` it replaces, every method of the map
 * synchronizes on the map itself, so a caller iterating the map must hold its monitor. Unlike it,
 * the key, value, and entry views are read-only, so every mutation goes through a method that
 * advances the count, and unsynchronized, so a caller must hold the monitor while using them.
 */
private[sql] final class ModificationCountingMap extends util.Map[String, String] {

  private val underlying = new util.HashMap[String, String]

  private val readOnlyUnderlying = util.Collections.unmodifiableMap[String, String](underlying)

  @volatile private var mutations = 0L

  /**
   * Number of mutating calls so far; a call may advance it without changing the entries. Values
   * derived from the entries stay valid while it is unchanged; use
   * [[snapshotWithModificationCount]] to pair a count with the entries it describes.
   */
  def modificationCount: Long = mutations

  /** A copy of the entries and the [[modificationCount]] they correspond to. */
  def snapshotWithModificationCount: (Long, util.Map[String, String]) = synchronized {
    (mutations, new util.HashMap[String, String](underlying))
  }

  private def mutate[T](f: => T): T = synchronized {
    mutations += 1
    f
  }

  override def size: Int = synchronized {
    underlying.size
  }

  override def isEmpty: Boolean = synchronized {
    underlying.isEmpty
  }

  override def containsKey(key: Any): Boolean = synchronized {
    underlying.containsKey(key)
  }

  override def containsValue(value: Any): Boolean = synchronized {
    underlying.containsValue(value)
  }

  override def get(key: Any): String = synchronized {
    underlying.get(key)
  }

  override def getOrDefault(key: Any, defaultValue: String): String = synchronized {
    underlying.getOrDefault(key, defaultValue)
  }

  override def forEach(action: BiConsumer[_ >: String, _ >: String]): Unit = synchronized {
    underlying.forEach(action)
  }

  override def put(key: String, value: String): String = mutate(underlying.put(key, value))

  override def remove(key: Any): String = mutate(underlying.remove(key))

  override def putAll(m: util.Map[_ <: String, _ <: String]): Unit = mutate(underlying.putAll(m))

  override def clear(): Unit = mutate(underlying.clear())

  // The java.util.Map defaults of the methods below are not atomic, and the default `replaceAll`
  // writes through the read-only entry view.
  override def replaceAll(function: BiFunction[_ >: String, _ >: String, _ <: String]): Unit =
    mutate(underlying.replaceAll(function))

  override def putIfAbsent(key: String, value: String): String =
    mutate(underlying.putIfAbsent(key, value))

  override def remove(key: Any, value: Any): Boolean = mutate(underlying.remove(key, value))

  override def replace(key: String, value: String): String = mutate(underlying.replace(key, value))

  override def replace(key: String, oldValue: String, newValue: String): Boolean =
    mutate(underlying.replace(key, oldValue, newValue))

  override def computeIfAbsent(
      key: String,
      mappingFunction: JFunction[_ >: String, _ <: String]): String =
    mutate(underlying.computeIfAbsent(key, mappingFunction))

  override def computeIfPresent(
      key: String,
      remappingFunction: BiFunction[_ >: String, _ >: String, _ <: String]): String =
    mutate(underlying.computeIfPresent(key, remappingFunction))

  override def compute(
      key: String,
      remappingFunction: BiFunction[_ >: String, _ >: String, _ <: String]): String =
    mutate(underlying.compute(key, remappingFunction))

  override def merge(
      key: String,
      value: String,
      remappingFunction: BiFunction[_ >: String, _ >: String, _ <: String]): String =
    mutate(underlying.merge(key, value, remappingFunction))

  override def keySet: util.Set[String] = readOnlyUnderlying.keySet

  override def values: util.Collection[String] = readOnlyUnderlying.values

  override def entrySet: util.Set[util.Map.Entry[String, String]] = readOnlyUnderlying.entrySet

  override def equals(o: Any): Boolean = synchronized {
    underlying.equals(o)
  }

  override def hashCode(): Int = synchronized {
    underlying.hashCode
  }

  override def toString(): String = synchronized {
    underlying.toString
  }
}
