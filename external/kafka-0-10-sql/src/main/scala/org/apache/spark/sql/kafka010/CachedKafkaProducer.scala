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

import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

private[kafka010] case class CachedKafkaProducer(id: String, inUseCount: AtomicInteger,
    kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]]) extends Logging {
  private var closed: Boolean = false
  private def close(): Unit = this.synchronized {
    if (!closed) {
      closed = true
      kafkaProducer.close()
      logInfo(s"Closed kafka producer: $kafkaProducer")
    }
  }
  private[kafka010] def flush(): Unit = {
    kafkaProducer.flush()
  }
  private[kafka010] def isClosed: Boolean = closed
}

private[kafka010] object CachedKafkaProducer extends Logging {

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long =
    Option(SparkEnv.get).map(_.conf.getTimeAsMs(
      "spark.kafka.producer.cache.timeout",
      s"${defaultCacheExpireTimeout}ms")).getOrElse(defaultCacheExpireTimeout)

  private val cacheLoader = new CacheLoader[Seq[(String, Object)], CachedKafkaProducer] {
    override def load(config: Seq[(String, Object)]): CachedKafkaProducer = {
      val configMap = config.map(x => x._1 -> x._2).toMap.asJava
      createKafkaProducer(configMap)
    }
  }

  private val closeQueue = new ConcurrentLinkedQueue[CachedKafkaProducer]()

  private val removalListener = new RemovalListener[Seq[(String, Object)], CachedKafkaProducer]() {
    override def onRemoval(
        notification: RemovalNotification[Seq[(String, Object)], CachedKafkaProducer]): Unit = {
      val producer: CachedKafkaProducer = notification.getValue
      logDebug(s"Evicting kafka producer $producer, due to ${notification.getCause}")
      if (producer.inUseCount.intValue() > 0) {
        // When a inuse producer is evicted we wait for it to be released before finally closing it.
        closeQueue.add(producer)
      } else {
        close(producer)
      }
    }
  }

  private lazy val guavaCache: LoadingCache[Seq[(String, Object)], CachedKafkaProducer] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[Seq[(String, Object)], CachedKafkaProducer](cacheLoader)

  private def createKafkaProducer(producerConfig: ju.Map[String, Object]): CachedKafkaProducer = {
    val updatedKafkaProducerConfiguration =
      KafkaConfigUpdater("executor", producerConfig.asScala.toMap)
        .setAuthenticationConfigIfNeeded()
        .build()
    val kafkaProducer =
      new KafkaProducer[Array[Byte], Array[Byte]](updatedKafkaProducerConfiguration)
    val id: String = ju.UUID.randomUUID().toString
    logDebug(s"Created a new instance of KafkaProducer for " +
      s"$updatedKafkaProducerConfiguration with Id: $id")
    CachedKafkaProducer(id, new AtomicInteger(0), kafkaProducer)
  }

  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  private[kafka010] def getOrCreate(kafkaParams: ju.Map[String, Object]): CachedKafkaProducer = {
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(kafkaParams)
    try {
      val cachedKafkaProducer: CachedKafkaProducer = guavaCache.get(paramsSeq)
      val useCount: Int = cachedKafkaProducer.inUseCount.incrementAndGet()
      logDebug(s"Granted producer $cachedKafkaProducer, inuse-count: $useCount")
      cachedKafkaProducer
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null =>
        throw e.getCause
    }
  }

  private def paramsToSeq(kafkaParams: ju.Map[String, Object]): Seq[(String, Object)] = {
    val paramsSeq: Seq[(String, Object)] = kafkaParams.asScala.toSeq.sortBy(x => x._1)
    paramsSeq
  }

  /** For explicitly closing kafka producer, will not close an inuse producer until released. */
  private[kafka010] def close(kafkaParams: ju.Map[String, Object]): Unit = {
    val paramsSeq = paramsToSeq(kafkaParams)
    guavaCache.invalidate(paramsSeq)
  }

  /** Close this producer and process pending closes. */
  private def close(producer: CachedKafkaProducer): Unit = synchronized {
    try {
      producer.close()
      // Check and close any producers evicted, and pending to be closed.
      for (p <- closeQueue.iterator().asScala) {
        if (p.inUseCount.intValue() <= 0) {
          producer.close()
          closeQueue.remove(p)
        }
      }
    } catch {
      case NonFatal(e) => logWarning(s"Error while closing kafka producer: $producer", e)
    }
  }

  // Intended for testing purpose only.
  private[kafka010] def clear(): Unit = {
    logInfo("Cleaning up guava cache and force closing all kafka producer.")
    guavaCache.invalidateAll()
    for (p <- closeQueue.iterator().asScala) {
      p.close()
    }
    closeQueue.clear()
  }

  private[kafka010] def getAsMap: ConcurrentMap[Seq[(String, Object)], CachedKafkaProducer] =
    guavaCache.asMap()
}
