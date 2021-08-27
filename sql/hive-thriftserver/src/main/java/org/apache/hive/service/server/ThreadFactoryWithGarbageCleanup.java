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


package org.apache.hive.service.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

import org.apache.hadoop.hive.metastore.RawStore;

/**
 * A ThreadFactory for constructing new HiveServer2 threads that lets you plug
 * in custom cleanup code to be called before this thread is GC-ed.
 * Currently cleans up the following:
 * 1. ThreadLocal RawStore object:
 * In case of an embedded metastore, HiveServer2 threads (foreground and background)
 * end up caching a ThreadLocal RawStore object. The ThreadLocal RawStore object has
 * an instance of PersistenceManagerFactory and PersistenceManager.
 * The PersistenceManagerFactory keeps a cache of PersistenceManager objects,
 * which are only removed when PersistenceManager#close method is called.
 * HiveServer2 uses ExecutorService for managing thread pools for foreground and background threads.
 * ExecutorService unfortunately does not provide any hooks to be called,
 * when a thread from the pool is terminated.
 * As a solution, we're using this ThreadFactory to keep a cache of RawStore objects per thread.
 * And we are doing clean shutdown in the finalizer for each thread.
 */
public class ThreadFactoryWithGarbageCleanup implements ThreadFactory {

  private static Map<Long, RawStore> threadRawStoreMap = new ConcurrentHashMap<Long, RawStore>();

  private final String namePrefix;

  public ThreadFactoryWithGarbageCleanup(String threadPoolName) {
    namePrefix = threadPoolName;
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread newThread = new ThreadWithGarbageCleanup(runnable);
    newThread.setName(namePrefix + ": Thread-" + newThread.getId());
    return newThread;
  }

  public static Map<Long, RawStore> getThreadRawStoreMap() {
    return threadRawStoreMap;
  }
}
