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

package org.apache.spark.util.kvstore;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.annotation.Private;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Collection;

/**
 * Implementation of KVStore that writes data to InMemoryStore at first and uses
 * a background thread to dump data to LevelDB after the writing to InMemoryStore
 * is completed.
 */
@Private
public class HybridStore implements KVStore {

  private InMemoryStore inMemoryStore = new InMemoryStore();
  private LevelDB levelDB = null;

  // Flag to indicate if we should use inMemoryStore Or levelDB.
  private AtomicBoolean shouldUseInMemoryStore = new AtomicBoolean(true);

  // A background thread that dumps data in inMemoryStore to levelDB
  private Thread backgroundThread = null;

  // A hash map that stores all class types (except CachedQuantile) that had been writen
  // to inMemoryStore.
  private ConcurrentHashMap<Class<?>, Boolean> klassMap = new ConcurrentHashMap<>();

  // CachedQuantile can be written to kvstore after rebuildAppStore(), so we need
  // to handle it specially to avoid conflicts. we will use a queue store CachedQuantile
  // objects when the underlying store is inMemoryStore, and dump these objects to levelDB
  // before the switch complete.
  private Class<?> cachedQuantileKlass = null;
  private ConcurrentLinkedQueue<Object> cachedQuantileQueue = new ConcurrentLinkedQueue<>();


  @Override
  public <T> T getMetadata(Class<T> klass) throws Exception {
    KVStore store = getStore();
    T metaData = store.getMetadata(klass);
    return metaData;
  }

  @Override
  public void setMetadata(Object value) throws Exception {
    KVStore store = getStore();
    store.setMetadata(value);
  }

  @Override
  public <T> T read(Class<T> klass, Object naturalKey) throws Exception {
    KVStore store = getStore();
    T value = store.read(klass, naturalKey);
    return value;
  }

  @Override
  public void write(Object value) throws Exception {
    Class<?> klass = value.getClass();

    if (backgroundThread != null && !klass.equals(cachedQuantileKlass)) {
      throw new RuntimeException("write() for objects other than CachedQuantile " +
        "shouldn't be called after the hybrid store begins switching to levelDB");
    }

    KVStore store = getStore();
    store.write(value);

    if (store instanceof InMemoryStore) {
      if (klass.equals(cachedQuantileKlass)) {
        cachedQuantileQueue.add(value);
      } else {
        klassMap.putIfAbsent(klass, true);
      }
    }
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) throws Exception {
    if (backgroundThread != null) {
      throw new RuntimeException("delete() shouldn't be called after " +
        "the hybrid store begins switching to levelDB");
    }

    KVStore store = getStore();
    store.delete(type, naturalKey);
  }

  @Override
  public <T> KVStoreView<T> view(Class<T> type) throws Exception {
    KVStore store = getStore();
    KVStoreView<T> view = store.view(type);
    return view;
  }

  @Override
  public long count(Class<?> type) throws Exception {
    KVStore store = getStore();
    long count = store.count(type);
    return count;
  }

  @Override
  public long count(Class<?> type, String index, Object indexedValue) throws Exception {
    KVStore store = getStore();
    long count = store.count(type, index, indexedValue);
    return count;
  }

  @Override
  public void close() throws IOException {
    try {
      if (backgroundThread != null && backgroundThread.isAlive()) {
        // The background thread is still running, wait for it to finish
        backgroundThread.join();
      }
      if (levelDB != null) {
        levelDB.close();
      }
      if (inMemoryStore != null) {
        inMemoryStore.close();
      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
    }
  }

  @Override
  public <T> boolean removeAllByIndexValues(
      Class<T> klass,
      String index,
      Collection<?> indexValues) throws Exception {
    if (backgroundThread != null) {
      throw new RuntimeException("removeAllByIndexValues() shouldn't be called after " +
        "the hybrid store begins switching to levelDB");
    }

    KVStore store = getStore();
    boolean removed = store.removeAllByIndexValues(klass, index, indexValues);
    return removed;
  }

  public void setLevelDB(LevelDB levelDB) {
    this.levelDB = levelDB;
  }

  public void setCachedQuantileKlass(Class<?> klass) {
    this.cachedQuantileKlass = klass;
  }

  /**
   * This method is called when the writing is done for inMemoryStore. A
   * background thread will be created and be started to dump data in inMemoryStore
   * to levelDB. Once the dumping is completed, the underlying kvstore will be
   * switched to levelDB.
   */
  public void switchingToLevelDB(SwitchingToLevelDBListener listener) throws Exception {
    // A background thread that dumps data to levelDB
    backgroundThread = new Thread(new Runnable() {
      public void run() {
        Exception exceptionCaught = null;

        try {
          for (Class<?> klass : klassMap.keySet()) {
            KVStoreIterator<?> it = inMemoryStore.view(klass).closeableIterator();
            while (it.hasNext()) {
              levelDB.write(it.next());
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          exceptionCaught = e;
        }

        if (exceptionCaught == null) {
          // Switch to levelDB and close inMemoryStore
          shouldUseInMemoryStore.set(false);

          // Dump CachedQuantile objects to levelDB
          try {
            while(cachedQuantileQueue.size() > 0) {
              levelDB.write(cachedQuantileQueue.poll());
            }
          } catch (Exception e) {
            e.printStackTrace();
          }

          inMemoryStore.close();
          listener.onSwitchingToLevelDBSuccess();
        } else {
          // Continue using inMemoryStore
          listener.onSwitchingToLevelDBFail(exceptionCaught);
        }
      }
    });

    backgroundThread.start();
  }

  /**
   * This method return the store that we should use.
   */
  private KVStore getStore() throws Exception {
    if (shouldUseInMemoryStore.get()) {
      return inMemoryStore;
    } else {
      return levelDB;
    }
  }

  public interface SwitchingToLevelDBListener {
    void onSwitchingToLevelDBSuccess();

    void onSwitchingToLevelDBFail(Exception e);
  }

  @VisibleForTesting boolean getShouldUseMemoryStore() {
    return this.shouldUseInMemoryStore.get();
  }

  @VisibleForTesting Thread getBackgroundThread() {
    return this.backgroundThread;
  }

}
