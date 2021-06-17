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

package org.apache.spark.deploy.history

import java.util.Collection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.google.common.collect.Lists;

import org.apache.spark.util.kvstore._

/**
 * An implementation of KVStore that accelerates event logs loading.
 *
 * When rebuilding the application state from event logs, HybridStore will
 * write data to InMemoryStore at first and use a background thread to dump
 * data to LevelDB once the app store is restored. We don't expect write
 * operations (except the case for caching) after calling switch to level DB.
 */

private[history] class HybridStore extends KVStore {

  private val inMemoryStore = new InMemoryStore()

  private var levelDB: LevelDB = null

  // Flag to indicate whether we should use inMemoryStore or levelDB
  private val shouldUseInMemoryStore = new AtomicBoolean(true)

  // Flag to indicate whether this hybrid store is closed, use this flag
  // to avoid starting background thread after the store is closed
  private val closed = new AtomicBoolean(false)

  // A background thread that dumps data from inMemoryStore to levelDB
  private var backgroundThread: Thread = null

  // A hash map that stores all classes that had been written to inMemoryStore
  // Visible for testing
  private[history] val klassMap = new ConcurrentHashMap[Class[_], Boolean]

  override def getMetadata[T](klass: Class[T]): T = {
    getStore().getMetadata(klass)
  }

  override def setMetadata(value: Object): Unit = {
    getStore().setMetadata(value)
  }

  override def read[T](klass: Class[T], naturalKey: Object): T = {
    getStore().read(klass, naturalKey)
  }

  override def write(value: Object): Unit = {
    getStore().write(value)

    if (backgroundThread == null) {
      // New classes won't be dumped once the background thread is started
      klassMap.putIfAbsent(value.getClass(), true)
    }
  }

  override def delete(klass: Class[_], naturalKey: Object): Unit = {
    if (backgroundThread != null) {
      throw new IllegalStateException("delete() shouldn't be called after " +
        "the hybrid store begins switching to levelDB")
    }

    getStore().delete(klass, naturalKey)
  }

  override def view[T](klass: Class[T]): KVStoreView[T] = {
    getStore().view(klass)
  }

  override def count(klass: Class[_]): Long = {
    getStore().count(klass)
  }

  override def count(klass: Class[_], index: String, indexedValue: Object): Long = {
    getStore().count(klass, index, indexedValue)
  }

  override def close(): Unit = {
    try {
      closed.set(true)
      if (backgroundThread != null && backgroundThread.isAlive()) {
        // The background thread is still running, wait for it to finish
        backgroundThread.join()
      }
    } finally {
      inMemoryStore.close()
      if (levelDB != null) {
        levelDB.close()
      }
    }
  }

  override def removeAllByIndexValues[T](
      klass: Class[T],
      index: String,
      indexValues: Collection[_]): Boolean = {
    if (backgroundThread != null) {
      throw new IllegalStateException("removeAllByIndexValues() shouldn't be " +
        "called after the hybrid store begins switching to levelDB")
    }

    getStore().removeAllByIndexValues(klass, index, indexValues)
  }

  def setLevelDB(levelDB: LevelDB): Unit = {
    this.levelDB = levelDB
  }

  /**
   * This method is called when the writing is done for inMemoryStore. A
   * background thread will be created and be started to dump data in inMemoryStore
   * to levelDB. Once the dumping is completed, the underlying kvstore will be
   * switched to levelDB.
   */
  def switchToLevelDB(
      listener: HybridStore.SwitchToLevelDBListener,
      appId: String,
      attemptId: Option[String]): Unit = {
    if (closed.get) {
      return
    }

    backgroundThread = new Thread(() => {
      try {
        for (klass <- klassMap.keys().asScala) {
          val values = Lists.newArrayList(
              inMemoryStore.view(klass).closeableIterator())
          levelDB.writeAll(values)
        }
        listener.onSwitchToLevelDBSuccess()
        shouldUseInMemoryStore.set(false)
        inMemoryStore.close()
      } catch {
        case e: Exception =>
          listener.onSwitchToLevelDBFail(e)
      }
    })
    backgroundThread.setDaemon(true)
    backgroundThread.setName(s"hybridstore-$appId-$attemptId")
    backgroundThread.start()
  }

  /**
   * This method return the store that we should use.
   * Visible for testing.
   */
  private[history] def getStore(): KVStore = {
    if (shouldUseInMemoryStore.get) {
      inMemoryStore
    } else {
      levelDB
    }
  }
}

private[history] object HybridStore {

  trait SwitchToLevelDBListener {

    def onSwitchToLevelDBSuccess(): Unit

    def onSwitchToLevelDBFail(e: Exception): Unit
  }
}
