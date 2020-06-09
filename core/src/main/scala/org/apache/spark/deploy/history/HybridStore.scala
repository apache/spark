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

import java.io.IOException
import java.util.Collection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.spark.util.kvstore._

/**
 * A implementation of KVStore that accelerates event logs loading.
 *
 * When rebuilding the application state from event logs, HybridStore will
 * write data to InMemoryStore at first and use a background thread to dump
 * data to LevelDB once the writing to InMemoryStore is completed.
 */

private[history] class HybridStore extends KVStore {

  private val inMemoryStore = new InMemoryStore()

  private var levelDB: LevelDB = null

  // Flag to indicate whether we should use inMemoryStore or levelDB
  private[history] val shouldUseInMemoryStore = new AtomicBoolean(true)

  // A background thread that dumps data from inMemoryStore to levelDB
  private var backgroundThread: Thread = null

  // A hash map that stores all classes that had been writen to inMemoryStore
  private val klassMap = new ConcurrentHashMap[Class[_], Boolean]

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
    val store = getStore()
    store.write(value)

    if (backgroundThread == null) {
      // New classes won't be dumped once the background thread is started
      klassMap.putIfAbsent(value.getClass(), true)
    }
  }

  override def delete(klass: Class[_], naturalKey: Object): Unit = {
    if (backgroundThread != null) {
      throw new RuntimeException("delete() shouldn't be called after " +
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
      if (backgroundThread != null && backgroundThread.isAlive()) {
        // The background thread is still running, wait for it to finish
        backgroundThread.join()
      }
      if (levelDB != null) {
        levelDB.close()
      }
    } catch {
      case ioe: IOException => throw ioe
    } finally {
      inMemoryStore.close()
    }
  }

  override def removeAllByIndexValues[T](
      klass: Class[T],
      index: String,
      indexValues: Collection[_]): Boolean = {
    if (backgroundThread != null) {
      throw new RuntimeException("removeAllByIndexValues() shouldn't be called after " +
        "the hybrid store begins switching to levelDB")
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
  def switchToLevelDB(listener: HybridStore.SwitchToLevelDBListener): Unit = {
    backgroundThread = new Thread(() => {
      var exception: Option[Exception] = None

      try {
        for (klass <- klassMap.keys().asScala) {
          val it = inMemoryStore.view(klass).closeableIterator()
          while (it.hasNext()) {
            levelDB.write(it.next())
          }
        }
      } catch {
        case e: Exception =>
          exception = Some(e)
      }

      exception match {
        case Some(e) =>
          listener.onSwitchToLevelDBFail(e)
        case None =>
          listener.onSwitchToLevelDBSuccess()
          shouldUseInMemoryStore.set(false)
          inMemoryStore.close()
      }
    })
    backgroundThread.setDaemon(true)
    backgroundThread.setName("hybridstore-switch-to-leveldb")
    backgroundThread.start()
  }

  /**
   * This method return the store that we should use.
   */
  private def getStore(): KVStore = {
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
