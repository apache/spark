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

package org.apache.spark.util

import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}

/**
 * SPARK-43300: Guava cache fate-sharing behavior might lead to unexpected cascade failure:
 * when multiple threads access the same key in the cache at the same time when the key is not in
 * the cache, Guava cache will block all requests and load the data only once. If the loading fails,
 * all requests will fail immediately without retry. Therefore individual failure will also fail
 * other irrelevant queries who are waiting for the same key. Given that spark can cancel tasks at
 * arbitrary times for many different reasons, fate sharing means that a task which gets canceled
 * while populating a cache entry can cause spurious failures in tasks from unrelated jobs -- even
 * though those tasks would have successfully populated the cache if they had been allowed to try.
 *
 * This util Cache wrapper with KeyLock to synchronize threads looking for the same key
 * so that they should run individually and fail as if they had arrived one at a time.
 *
 * There are so many ways to add cache entries in Guava Cache, instead of implementing Guava Cache
 * and LoadingCache interface, we expose a subset of APIs so that we can control at compile time
 * what cache operations are allowed.
 */
private[spark] object NonFateSharingCache {
  /**
   * This will return a NonFateSharingLoadingCache instance if user happens to pass a LoadingCache
   */
  def apply[K, V](cache: Cache[K, V]): NonFateSharingCache[K, V] = cache match {
    case loadingCache: LoadingCache[K, V] => apply(loadingCache)
    case _ => new NonFateSharingCache(cache)
  }

  def apply[K, V](loadingCache: LoadingCache[K, V]): NonFateSharingLoadingCache[K, V] =
    new NonFateSharingLoadingCache(loadingCache)

  /**
   * SPARK-44064 add this `apply` function to break non-core modules code directly using
   * Guava Cache related types as input parameter to invoke other `NonFateSharingCache#apply`
   * function, which can avoid non-core modules Maven test failures caused by using
   * shaded core module.
   * We should refactor this function to be more general when there are other requirements,
   * or remove this function when Maven testing is no longer supported.
   */
  def apply[K, V](loadingFunc: K => V, maximumSize: Long = 0L): NonFateSharingLoadingCache[K, V] = {
    require(loadingFunc != null)
    val builder = CacheBuilder.newBuilder().asInstanceOf[CacheBuilder[K, V]]
    if (maximumSize > 0L) {
      builder.maximumSize(maximumSize)
    }
    new NonFateSharingLoadingCache(builder.build[K, V](new CacheLoader[K, V] {
      override def load(k: K): V = loadingFunc.apply(k)
    }))
  }

  def apply[K, V](
      maximumSize: Long,
      expireAfterAccessTime: Long,
      expireAfterAccessTimeUnit: TimeUnit): NonFateSharingCache[K, V] = {
    val builder = CacheBuilder.newBuilder().asInstanceOf[CacheBuilder[K, V]]
    if (maximumSize > 0L) {
      builder.maximumSize(maximumSize)
    }
    if(expireAfterAccessTime > 0) {
      builder.expireAfterAccess(expireAfterAccessTime, expireAfterAccessTimeUnit)
    }
    new NonFateSharingCache(builder.build[K, V]())
  }
}

private[spark] class NonFateSharingCache[K, V](protected val cache: Cache[K, V]) {

  protected val keyLock = new KeyLock[K]

  def get(key: K, valueLoader: Callable[_ <: V]): V = keyLock.withLock(key) {
    cache.get(key, valueLoader)
  }

  def getIfPresent(key: Any): V = cache.getIfPresent(key)

  def invalidate(key: Any): Unit = cache.invalidate(key)

  def invalidateAll(): Unit = cache.invalidateAll()

  def size(): Long = cache.size()
}

private[spark] class NonFateSharingLoadingCache[K, V](
  protected val loadingCache: LoadingCache[K, V]) extends NonFateSharingCache[K, V](loadingCache) {

  def get(key: K): V = keyLock.withLock(key) {
    loadingCache.get(key)
  }
}
