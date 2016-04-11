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
package org.apache.spark.storage.memory

import java.util

import scala.collection.mutable.ArrayBuffer

trait MemoryEntryManager[K, V] {
  def getEntry(blockId: K): V

  def putEntry(key: K, value: V): V

  def removeEntry(key: K): V

  def clear()

  def containsEntry(key: K): Boolean
}

class FIFOMemoryEntryManager[K, V] extends MemoryEntryManager[K, V] {
  val entries = new util.LinkedHashMap[K, V](32, 0.75f)

  override def getEntry(key: K): V = {
    entries.synchronized {
      entries.get(key)
    }
  }

  override def putEntry(key: K, value: V): V = {
    entries.synchronized {
      entries.put(key, value)
    }
  }

  def clear() {
    entries.synchronized {
      entries.clear()
    }
  }

  override def removeEntry(key: K): V = {
    entries.synchronized {
      entries.remove(key)
    }
  }

  override def containsEntry(key: K): Boolean = {
    entries.synchronized {
      entries.containsKey(key)
    }
  }
}

class LRUMemoryEntryManager[K, V] extends MemoryEntryManager[K, V] {
  def entrySet() : util.Set[util.Map.Entry[K, V]] = {
    entries.entrySet()
  }

  val entries = new util.LinkedHashMap[K, V](32, 0.75f, true)

  override def getEntry(key: K): V = {
    entries.synchronized {
      entries.get(key)
    }
  }

  override def putEntry(key: K, value: V): V = {
    entries.synchronized {
      entries.put(key, value)
    }
  }

  def clear() {
    entries.synchronized {
      entries.clear()
    }
  }

  override def removeEntry(key: K): V = {
    entries.synchronized {
      entries.remove(key)
    }
  }

  override def containsEntry(key: K): Boolean = {
    entries.synchronized {
      entries.containsKey(key)
    }
  }
}