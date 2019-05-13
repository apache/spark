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
import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentMap, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.kafka.clients.producer.KafkaProducer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.KafkaConfigUpdater

private[kafka010] case class CachedKafkaProducer(
    private val id: String = ju.UUID.randomUUID().toString,
    private val inUseCount: AtomicInteger = new AtomicInteger(0),
    private val kafkaParams: Seq[(String, Object)]) extends Logging {

  private val configMap = kafkaParams.map(x => x._1 -> x._2).toMap.asJava

  lazy val kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](configMap)
    logDebug(s"Created a new instance of KafkaProducer for " +
      s"$kafkaParams with Id: $id")
    closed = false
    producer
  }
  @GuardedBy("this")
  private var closed: Boolean = true
  private def close(): Unit = {
    try {
      this.synchronized {
        if (!closed) {
          closed = true
          kafkaProducer.close()
          logDebug(s"Closed kafka producer: $this")
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error while closing kafka producer with params: $kafkaParams", e)
    }
  }

  private def inUse(): Boolean = inUseCount.get() > 0

  private[kafka010] def getInUseCount: Int = inUseCount.get()

  private[kafka010] def getKafkaParams: Seq[(String, Object)] = kafkaParams

  private[kafka010] def flush(): Unit = kafkaProducer.flush()

  private[kafka010] def isClosed: Boolean = closed
}

private[kafka010] object CachedKafkaProducer extends Logging {

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long = Option(SparkEnv.get)
    .map(_.conf.get(PRODUCER_CACHE_TIMEOUT))
    .getOrElse(defaultCacheExpireTimeout)

  private val cacheLoader = new CacheLoader[Seq[(String, Object)], CachedKafkaProducer] {
    override def load(params: Seq[(String, Object)]): CachedKafkaProducer = {
      CachedKafkaProducer(kafkaParams = params)
    }
  }

  private def updatedAuthConfigIfNeeded(kafkaParamsMap: ju.Map[String, Object]) =
    KafkaConfigUpdater("executor", kafkaParamsMap.asScala.toMap)
      .setAuthenticationConfigIfNeeded()
      .build()

  private val closeQueue = new ConcurrentLinkedQueue[CachedKafkaProducer]()

  private val removalListener = new RemovalListener[Seq[(String, Object)], CachedKafkaProducer]() {
    override def onRemoval(
        notification: RemovalNotification[Seq[(String, Object)], CachedKafkaProducer]): Unit = {
      val producer: CachedKafkaProducer = notification.getValue
      logDebug(s"Evicting kafka producer $producer, due to ${notification.getCause}.")
      if (producer.inUse()) {
        // When `inuse` producer is evicted we wait for it to be released by all the tasks,
        // before finally closing it.
        closeQueue.add(producer)
      } else {
        producer.close()
      }
    }
  }

  private lazy val guavaCache: LoadingCache[Seq[(String, Object)], CachedKafkaProducer] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[Seq[(String, Object)], CachedKafkaProducer](cacheLoader)


  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  private[kafka010] def acquire(kafkaParamsMap: ju.Map[String, Object]): CachedKafkaProducer = {
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(updatedAuthConfigIfNeeded(kafkaParamsMap))
    try {
      val producer = this.synchronized {
        val cachedKafkaProducer: CachedKafkaProducer = guavaCache.get(paramsSeq)
        cachedKafkaProducer.inUseCount.incrementAndGet()
        logDebug(s"Granted producer $cachedKafkaProducer")
        cachedKafkaProducer
      }
      producer
    } catch {
      case e@(_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null =>
        throw e.getCause
    }
  }

  private def paramsToSeq(kafkaParamsMap: ju.Map[String, Object]): Seq[(String, Object)] = {
    val paramsSeq: Seq[(String, Object)] = kafkaParamsMap.asScala.toSeq.sortBy(x => x._1)
    paramsSeq
  }

  /* Release a kafka producer back to the kafka cache. We simply decrement it's inuse count. */
  private[kafka010] def release(producer: CachedKafkaProducer, failing: Boolean): Unit = {
    this.synchronized {
      // It should be ok to call release multiple times on the same producer object.
      if (producer.inUse()) {
        producer.inUseCount.decrementAndGet()
        logDebug(s"Released producer $producer.")
      } else {
        logWarning(s"Tried to release a not in use producer, $producer.")
      }
      if (failing) {
        // If this producer is failing to write, we remove it from cache.
        // So that it is re-created, eventually.
        val cachedProducer = guavaCache.getIfPresent(producer.kafkaParams)
        if (cachedProducer != null && cachedProducer.id == producer.id) {
          logDebug(s"Invalidating a failing producer: $producer.")
          guavaCache.invalidate(producer.kafkaParams)
        }
      }
    }
    // We need a close queue, so that we can close the producer(s) outside of a synchronized block.
    processPendingClose()
  }

  /** Process pending closes. */
  private def processPendingClose(): Unit = {
    // Check and close any other producers previously evicted, but pending to be closed.
    for (p <- closeQueue.iterator().asScala) {
      if (!p.inUse()) {
        closeQueue.remove(p)
        p.close()
      }
    }
  }

  // For testing only.
  private[kafka010] def clear(): Unit = {
    logInfo("Cleaning up guava cache and force closing all kafka producers.")
    guavaCache.invalidateAll()
    for (p <- closeQueue.iterator().asScala) {
      p.close()
    }
    closeQueue.clear()
  }

  // For testing only.
  private[kafka010] def evict(params: Seq[(String, Object)]): Unit = {
    guavaCache.invalidate(params)
  }

  // For testing only.
  private[kafka010] def getAsMap: ConcurrentMap[Seq[(String, Object)], CachedKafkaProducer] =
    guavaCache.asMap()
}
