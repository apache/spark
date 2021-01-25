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

package org.apache.spark.sql.kafka010.consumer

import java.{util => ju}
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.LongAdder

import scala.collection.mutable

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.{FETCHED_DATA_CACHE_EVICTOR_THREAD_RUN_INTERVAL, FETCHED_DATA_CACHE_TIMEOUT}
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer.{AvailableOffsetRange, CacheKey, UNKNOWN_OFFSET}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * Provides object pool for [[FetchedData]] which is grouped by [[CacheKey]].
 *
 * Along with CacheKey, it receives desired start offset to find cached FetchedData which
 * may be stored from previous batch. If it can't find one to match, it will create
 * a new FetchedData. As "desired start offset" plays as second level of key which can be
 * modified in same instance, this class cannot be replaced with general pool implementations
 * including Apache Commons Pool which pools KafkaConsumer.
 */
private[consumer] class FetchedDataPool(
    executorService: ScheduledExecutorService,
    clock: Clock,
    conf: SparkConf) extends Logging {
  import FetchedDataPool._

  def this(sparkConf: SparkConf) = {
    this(
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kafka-fetched-data-cache-evictor"), new SystemClock, sparkConf)
  }

  private val cache: mutable.Map[CacheKey, CachedFetchedDataList] = mutable.HashMap.empty

  private val minEvictableIdleTimeMillis = conf.get(FETCHED_DATA_CACHE_TIMEOUT)
  private val evictorThreadRunIntervalMillis =
    conf.get(FETCHED_DATA_CACHE_EVICTOR_THREAD_RUN_INTERVAL)

  private def startEvictorThread(): Option[ScheduledFuture[_]] = {
    if (evictorThreadRunIntervalMillis > 0) {
      val future = executorService.scheduleAtFixedRate(() => {
        Utils.tryLogNonFatalError(removeIdleFetchedData())
      }, 0, evictorThreadRunIntervalMillis, TimeUnit.MILLISECONDS)
      Some(future)
    } else {
      None
    }
  }

  private var scheduled = startEvictorThread()

  private val numCreatedFetchedData = new LongAdder()
  private val numTotalElements = new LongAdder()

  def numCreated: Long = numCreatedFetchedData.sum()
  def numTotal: Long = numTotalElements.sum()

  def acquire(key: CacheKey, desiredStartOffset: Long): FetchedData = synchronized {
    val fetchedDataList = cache.getOrElseUpdate(key, new CachedFetchedDataList())

    val cachedFetchedDataOption = fetchedDataList.find { p =>
      !p.inUse && p.getObject.nextOffsetInFetchedData == desiredStartOffset
    }

    var cachedFetchedData: CachedFetchedData = null
    if (cachedFetchedDataOption.isDefined) {
      cachedFetchedData = cachedFetchedDataOption.get
    } else {
      cachedFetchedData = CachedFetchedData.empty()
      fetchedDataList += cachedFetchedData

      numCreatedFetchedData.increment()
      numTotalElements.increment()
    }

    cachedFetchedData.lastAcquiredTimestamp = clock.getTimeMillis()
    cachedFetchedData.inUse = true

    cachedFetchedData.getObject
  }

  def invalidate(key: CacheKey): Unit = synchronized {
    cache.remove(key) match {
      case Some(lst) => numTotalElements.add(-1 * lst.size)
      case None =>
    }
  }

  def release(key: CacheKey, fetchedData: FetchedData): Unit = synchronized {
    def warnReleasedDataNotInPool(key: CacheKey, fetchedData: FetchedData): Unit = {
      logWarning(s"No matching data in pool for $fetchedData in key $key. " +
        "It might be released before, or it was not a part of pool.")
    }

    cache.get(key) match {
      case Some(fetchedDataList) =>
        val cachedFetchedDataOption = fetchedDataList.find { p =>
          p.inUse && p.getObject == fetchedData
        }

        if (cachedFetchedDataOption.isEmpty) {
          warnReleasedDataNotInPool(key, fetchedData)
        } else {
          val cachedFetchedData = cachedFetchedDataOption.get
          cachedFetchedData.inUse = false
          cachedFetchedData.lastReleasedTimestamp = clock.getTimeMillis()
        }

      case None =>
        warnReleasedDataNotInPool(key, fetchedData)
    }
  }

  def shutdown(): Unit = {
    ThreadUtils.shutdown(executorService)
  }

  def reset(): Unit = synchronized {
    scheduled.foreach(_.cancel(true))

    cache.clear()
    numTotalElements.reset()
    numCreatedFetchedData.reset()

    scheduled = startEvictorThread()
  }

  private def removeIdleFetchedData(): Unit = synchronized {
    val now = clock.getTimeMillis()
    val maxAllowedReleasedTimestamp = now - minEvictableIdleTimeMillis
    cache.values.foreach { p: CachedFetchedDataList =>
      val expired = p.filter { q =>
        !q.inUse && q.lastReleasedTimestamp < maxAllowedReleasedTimestamp
      }
      p --= expired
      numTotalElements.add(-1 * expired.size)
    }
  }
}

private[consumer] object FetchedDataPool {
  private[consumer] case class CachedFetchedData(fetchedData: FetchedData) {
    var lastReleasedTimestamp: Long = Long.MaxValue
    var lastAcquiredTimestamp: Long = Long.MinValue
    var inUse: Boolean = false

    def getObject: FetchedData = fetchedData
  }

  private object CachedFetchedData {
    def empty(): CachedFetchedData = {
      val emptyData = FetchedData(
        ju.Collections.emptyListIterator[ConsumerRecord[Array[Byte], Array[Byte]]],
        UNKNOWN_OFFSET,
        UNKNOWN_OFFSET,
        AvailableOffsetRange(UNKNOWN_OFFSET, UNKNOWN_OFFSET))

      CachedFetchedData(emptyData)
    }
  }

  private[consumer] type CachedFetchedDataList = mutable.ListBuffer[CachedFetchedData]
}
