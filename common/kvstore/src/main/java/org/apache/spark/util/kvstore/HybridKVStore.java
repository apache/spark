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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Collection;

/**
 * Implementation of KVStore that keeps data deserialized in memory at first and having
 * a background thread that writes to the levelDB.
 */
@Private
public class HybridKVStore implements KVStore {
  /** In memory store */
  private InMemoryStore inMemoryStore = new InMemoryStore();

  /** Level DB */
  private LevelDB levelDB = null;

  /** Flag to indicate if we should use InMemoryStore Or LevelDB */
  private AtomicBoolean shouldUseInMemoryStore = new AtomicBoolean(true);

  /** Queue of pending operations that need to perform to levelDB */
  private LinkedBlockingQueue<LevelDBOperation> pendingLevelDBOperationsQueue =
    new LinkedBlockingQueue<>();

  /** Number of threads that is currently using inMemoryStore. This is to prevent
   * the case closing inMemoryStore while some threads is using it. */
  private AtomicLong inMemoryStoreUseCount = new AtomicLong(0);

  /** Background thread that performs operations in pendingLevelDBOperationsQueue. */
  private Thread writingToLevelDBThread;

  /** This flag indicate if we encountered any exception while modifying inMemoryStore. */
  private AtomicBoolean isEncounteredException = new AtomicBoolean(false);

  @Override
  public <T> T getMetadata(Class<T> klass) throws Exception {
    KVStore store = getStore(false);
    T metaData = store.getMetadata(klass);
    if (store instanceof InMemoryStore) {
      // Close memory store if we completely switched to use levelDB
      inMemoryStoreUseCount.decrementAndGet();
      maybeCloseInMemoryStore();
    }
    return metaData;
  }

  @Override
  public void setMetadata(Object value) throws Exception {
    try {
      KVStore store = getStore(true);
      store.setMetadata(value);

      if (store instanceof InMemoryStore) {
        // Also try set metadata for levelDB
        if (levelDB != null) {
          levelDB.setMetadata(value);
        }
        // Close memory store if we completely switched to use levelDB
        inMemoryStoreUseCount.decrementAndGet();
        maybeCloseInMemoryStore();
      }
    } catch (Exception e) {
      isEncounteredException.set(true);
      throw e;
    }
  }

  @Override
  public <T> T read(Class<T> klass, Object naturalKey) throws Exception {
    KVStore store = getStore(false);
    T value = store.read(klass, naturalKey);

    if (store instanceof InMemoryStore) {
      // Close memory store if we completely switched to use levelDB
      inMemoryStoreUseCount.decrementAndGet();
      maybeCloseInMemoryStore();
    }
    return value;
  }

  @Override
  public void write(Object value) throws Exception {
    try {
      KVStore store = getStore(true);
      store.write(value);
      if (store instanceof InMemoryStore) {
        if (writingToLevelDBThread != null && writingToLevelDBThread.isAlive()) {
          // Puting this write operation to the queue for the background thread to processes
          pendingLevelDBOperationsQueue.put(
            new LevelDBOperation(LevelDBOperationType.WRITE, value));
        }

        // Close memory store if we completely switched to use levelDB
        inMemoryStoreUseCount.decrementAndGet();
        maybeCloseInMemoryStore();
      }
    } catch (Exception e) {
      isEncounteredException.set(true);
      throw e;
    }
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) throws Exception {
    try {
      KVStore store = getStore(true);
      store.delete(type, naturalKey);
      if (store instanceof InMemoryStore && levelDB != null) {
        if (writingToLevelDBThread != null && writingToLevelDBThread.isAlive()) {
          // Puting this delete operation to the queue for the background thread to processes
          pendingLevelDBOperationsQueue.put(
            new LevelDBOperation(LevelDBOperationType.DELETE, type, naturalKey));
        }

        // Close memory store if we completely switched to use levelDB
        inMemoryStoreUseCount.decrementAndGet();
        maybeCloseInMemoryStore();
      }
    } catch (Exception e) {
      isEncounteredException.set(true);
      throw e;
    }
  }

  @Override
  public <T> KVStoreView<T> view(Class<T> type) throws Exception {
    KVStore store = getStore(false);
    KVStoreView<T> kvStoreView = store.view(type);

    if (store instanceof InMemoryStore) {
      // Close memory store if we completely switched to use levelDB
      inMemoryStoreUseCount.decrementAndGet();
      maybeCloseInMemoryStore();
    }
    return kvStoreView;
  }

  @Override
  public long count(Class<?> type) throws Exception {
    KVStore store = getStore(false);
    long count = store.count(type);

    if (store instanceof InMemoryStore) {
      // Close memory store if we completely switched to use levelDB
      inMemoryStoreUseCount.decrementAndGet();
      maybeCloseInMemoryStore();
    }
    return count;
  }

  @Override
  public long count(Class<?> type, String index, Object indexedValue) throws Exception {
    KVStore store = getStore(false);
    long count = store.count(type, index, indexedValue);

    if (store instanceof InMemoryStore) {
      // Close memory store if we completely switched to use levelDB
      inMemoryStoreUseCount.decrementAndGet();
      maybeCloseInMemoryStore();
    }
    return count;
  }

  @Override
  public void close() throws IOException {
    try {
      if (writingToLevelDBThread != null &&
              writingToLevelDBThread.isAlive()) {
        // If background thread is still running, wait for it to finish
        pendingLevelDBOperationsQueue.put(
          new LevelDBOperation(LevelDBOperationType.STOP_BACKGROUND_THREAD));
        writingToLevelDBThread.join();
        levelDB.close();
      } else if(levelDB != null) {
        // HybridKVStore has switched to levelDB mode
        levelDB.close();
      }
    } catch (Exception e) {
      // Only throw the exception if it's IOException, which is thrown by levelDB.close()
      if (e instanceof IOException) {
        throw (IOException) e;
      }
    } finally {
      inMemoryStore.close();
    }
  }

  @Override
  public <T> boolean removeAllByIndexValues(
      Class<T> klass,
      String index,
      Collection<?> indexValues) throws Exception {
    try {
      KVStore store = getStore(true);
      boolean removed = store.removeAllByIndexValues(klass, index, indexValues);
      if (store instanceof InMemoryStore && levelDB != null) {
        if (writingToLevelDBThread != null && writingToLevelDBThread.isAlive()) {
          // Puting this removeAllByIndexValues operation to the queue
          // for the background thread to processes.
          pendingLevelDBOperationsQueue.put(
            new LevelDBOperation(LevelDBOperationType.REMOVE_ALL_BY_INDEX_VALUES,
              klass, index, indexValues));
        }

        // Close memory store if we completely switched to use levelDB
        inMemoryStoreUseCount.decrementAndGet();
        maybeCloseInMemoryStore();
      }
      return removed;
    } catch (Exception e) {
      isEncounteredException.set(true);
      throw e;
    }
  }

  /**
   * This method will send signal to stop the writingToLevelDBThread. And we will
   * use levelDB when all the pending operations finished. Note that this method
   * is return immediately, and we will keep using inMemoryStore until the
   * writingToLevelDBThread is stopped.
   */
  public void stopBackgroundThreadAndSwitchToLevelDB() throws Exception {
    assert (this.levelDB != null);
    if (writingToLevelDBThread != null && writingToLevelDBThread.isAlive()) {
      pendingLevelDBOperationsQueue.put(
        new LevelDBOperation(LevelDBOperationType.STOP_BACKGROUND_THREAD));
    }
  }


  /** This method set the levelDB that will be used. */
  public void setLevelDB(LevelDB levelDB_) {
    this.levelDB = levelDB_;
  }

  /** This method start the background thread to write to levelDB. */
  public synchronized void startBackgroundThreadToWriteToDB(SwitchingToLevelDBListener myListener) {
    if (writingToLevelDBThread == null || !writingToLevelDBThread.isAlive()) {
      writingToLevelDBThread = new Thread(new Runnable() {
        @Override
        public void run() {
          boolean shouldStop = false;
          try {
            // Loop to processing operations in pendingLevelDBOperationsQueue
            while(!shouldStop) {
              LevelDBOperation operation = pendingLevelDBOperationsQueue.take();

              if (operation.operationType == LevelDBOperationType.WRITE) {
                levelDB.write(operation.value);
              } else if (operation.operationType == LevelDBOperationType.DELETE) {
                levelDB.delete(operation.type, operation.naturalKey);
              } else if (operation.operationType ==
                  LevelDBOperationType.REMOVE_ALL_BY_INDEX_VALUES) {
                levelDB.removeAllByIndexValues(operation.klass, operation.index,
                  operation.indexValues);
              } else {
                shouldStop = true;
              }
            }
            if (isEncounteredException.get()) {
              // Notify the caller to commit the lease when success
              myListener.onSwitchingToLevelDBFail(
                new Exception("Encountered exception while using inMemoryStore"));
            } else {
              // Notify the caller to commit the lease when success
              myListener.onSwitchingToLevelDBSuccess();
              // Set flag for starting use leveldb
              shouldUseInMemoryStore.set(false);
              // Clear the queue in case there is any operations left
              pendingLevelDBOperationsQueue.clear();
              // Close the inMemoryStore if no one is using it
              maybeCloseInMemoryStore();
            }
          } catch (Exception e) {
            // Clear the operations queue
            pendingLevelDBOperationsQueue.clear();
            myListener.onSwitchingToLevelDBFail(e);
          }
        }
      });
      writingToLevelDBThread.start();
    }
  }

  /**
   * This method return the store that we should use. If we need to write to levelDB,
   * need to make sure that the background thread is stopped.
   */
  private KVStore getStore(boolean isWriting) throws Exception {
    if (shouldUseInMemoryStore.get()) {
      inMemoryStoreUseCount.incrementAndGet();
      return inMemoryStore;
    } else {
      if (isWriting) {
        if (writingToLevelDBThread != null && writingToLevelDBThread.isAlive()) {
          writingToLevelDBThread.join();
        }
        return levelDB;
      } else {
        return levelDB;
      }
    }
  }

  /**
   * This method close the InMemoryStore if there's no thread is using it and
   * the shouldUseInMemoryStore==false.
   */
  private void maybeCloseInMemoryStore() {
    if (!shouldUseInMemoryStore.get() && inMemoryStoreUseCount.get() <= 0) {
      inMemoryStore.close();
    }
  }

  public interface SwitchingToLevelDBListener {
    void onSwitchingToLevelDBSuccess();
    void onSwitchingToLevelDBFail(Exception e);
  }

  private enum LevelDBOperationType {WRITE, DELETE, STOP_BACKGROUND_THREAD,
    REMOVE_ALL_BY_INDEX_VALUES}

  private class LevelDBOperation<T> {
    LevelDBOperationType operationType;
    Class<?> type;
    Object naturalKey;
    Object value;
    Class<T> klass;
    String index;
    Collection<?> indexValues;

    LevelDBOperation(LevelDBOperationType operationType) {
      this.operationType = operationType;
    }

    // This is for delete operation
    LevelDBOperation(LevelDBOperationType operationType, Class<?> type, Object naturalKey) {
      this.operationType = operationType;
      this.type = type;
      this.naturalKey = naturalKey;
    }

    // This is for write operation
    LevelDBOperation(LevelDBOperationType operationType, Object value) {
      this.operationType = operationType;
      this.value = value;
    }

    // This is for removeAllByIndexValues operation
    LevelDBOperation(
      LevelDBOperationType operationType,
      Class<T> klass, String index,
      Collection<?> indexValues) {
      this.operationType = operationType;
      this.klass = klass;
      this.index = index;
      this.indexValues = indexValues;
    }
  }

  @VisibleForTesting boolean getShouldUseMemoryStore() {
    return this.shouldUseInMemoryStore.get();
  }

  @VisibleForTesting Thread getWritingToLevelDBThread() {
    return this.writingToLevelDBThread;
  }
}
