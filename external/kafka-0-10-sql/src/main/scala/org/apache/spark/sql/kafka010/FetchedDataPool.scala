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

package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.LongAdder

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.KafkaDataConsumer.{CacheKey, UNKNOWN_OFFSET}
import org.apache.spark.util.ThreadUtils

/**
 * Provides object pool for [[FetchedData]] which is grouped by [[CacheKey]].
 *
 * Along with CacheKey, it receives desired start offset to find cached FetchedData which
 * may be stored from previous batch. If it can't find one to match, it will create
 * a new FetchedData.
 */
private[kafka010] class FetchedDataPool extends Logging {
  import FetchedDataPool._

  private val cache: mutable.Map[CacheKey, CachedFetchedDataList] = mutable.HashMap.empty

  private val (minEvictableIdleTimeMillis, evictorThreadRunIntervalMillis): (Long, Long) = {
    val conf = SparkEnv.get.conf

    val minEvictIdleTime = conf.getLong(CONFIG_NAME_MIN_EVICTABLE_IDLE_TIME_MILLIS,
      DEFAULT_VALUE_MIN_EVICTABLE_IDLE_TIME_MILLIS)

    val evictorThreadInterval = conf.getLong(
      CONFIG_NAME_EVICTOR_THREAD_RUN_INTERVAL_MILLIS,
      DEFAULT_VALUE_EVICTOR_THREAD_RUN_INTERVAL_MILLIS)

    (minEvictIdleTime, evictorThreadInterval)
  }

  private val executorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "kafka-fetched-data-cache-evictor")

  private def startEvictorThread(): ScheduledFuture[_] = {
    executorService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        try {
          removeIdleFetchedData()
        } catch {
          case NonFatal(e) =>
            logWarning("Exception occurred while removing idle fetched data.", e)
        }
      }
    }, 0, evictorThreadRunIntervalMillis, TimeUnit.MILLISECONDS)
  }

  private var scheduled = startEvictorThread()

  private val numCreatedFetchedData = new LongAdder()
  private val numTotalElements = new LongAdder()

  def getNumCreated: Long = numCreatedFetchedData.sum()
  def getNumTotal: Long = numTotalElements.sum()

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

    cachedFetchedData.lastAcquiredTimestamp = System.currentTimeMillis()
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
    cache.get(key) match {
      case Some(fetchedDataList) =>
        val cachedFetchedDataOption = fetchedDataList.find { p =>
          p.inUse && p.getObject == fetchedData
        }

        if (cachedFetchedDataOption.isDefined) {
          val cachedFetchedData = cachedFetchedDataOption.get
          cachedFetchedData.inUse = false
          cachedFetchedData.lastReleasedTimestamp = System.currentTimeMillis()
        }

      case None => logWarning(s"No matching data in pool for $fetchedData in key $key. " +
        "It might be released before, or it was not a part of pool.")
    }
  }

  def shutdown(): Unit = {
    executorService.shutdownNow()
  }

  def reset(): Unit = synchronized {
    scheduled.cancel(true)

    cache.clear()
    numTotalElements.reset()
    numCreatedFetchedData.reset()

    scheduled = startEvictorThread()
  }

  private def removeIdleFetchedData(): Unit = synchronized {
    val timestamp = System.currentTimeMillis()
    val maxAllowedIdleTimestamp = timestamp - minEvictableIdleTimeMillis
    cache.values.foreach { p: CachedFetchedDataList =>
      val idles = p.filter(q => !q.inUse && q.lastReleasedTimestamp < maxAllowedIdleTimestamp)
      val lstSize = p.size
      idles.foreach(idle => p -= idle)
      numTotalElements.add(-1 * (lstSize - p.size))
    }
  }
}

private[kafka010] object FetchedDataPool {
  private[kafka010] case class CachedFetchedData(fetchedData: FetchedData) {
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
        UNKNOWN_OFFSET)

      CachedFetchedData(emptyData)
    }
  }

  private[kafka010] type CachedFetchedDataList = mutable.ListBuffer[CachedFetchedData]

  val CONFIG_NAME_PREFIX = "spark.sql.kafkaFetchedDataCache."
  val CONFIG_NAME_MIN_EVICTABLE_IDLE_TIME_MILLIS = CONFIG_NAME_PREFIX +
    "minEvictableIdleTimeMillis"
  val CONFIG_NAME_EVICTOR_THREAD_RUN_INTERVAL_MILLIS = CONFIG_NAME_PREFIX +
    "evictorThreadRunIntervalMillis"

  val DEFAULT_VALUE_MIN_EVICTABLE_IDLE_TIME_MILLIS = 10 * 60 * 1000 // 10 minutes
  val DEFAULT_VALUE_EVICTOR_THREAD_RUN_INTERVAL_MILLIS = 5 * 60 * 1000 // 3 minutes

  def build: FetchedDataPool = new FetchedDataPool()
}
