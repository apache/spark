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

package org.apache.spark.sql.kafka010.producer

import java.{util => ju}
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kafka.clients.producer.KafkaProducer

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.{KafkaConfigUpdater, KafkaRedactionUtil}
import org.apache.spark.sql.kafka010.{PRODUCER_CACHE_EVICTOR_THREAD_RUN_INTERVAL, PRODUCER_CACHE_TIMEOUT}
import org.apache.spark.util.{Clock, ShutdownHookManager, SystemClock, ThreadUtils, Utils}

/**
 * Provides object pool for [[CachedKafkaProducer]] which is grouped by
 * [[org.apache.spark.sql.kafka010.producer.InternalKafkaProducerPool.CacheKey]].
 */
private[producer] class InternalKafkaProducerPool(
    executorService: ScheduledExecutorService,
    val clock: Clock,
    conf: SparkConf) extends Logging {
  import InternalKafkaProducerPool._

  def this(sparkConf: SparkConf) = {
    this(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kafka-producer-cache-evictor"), new SystemClock, sparkConf)
  }

  /** exposed for testing */
  private[producer] val cacheExpireTimeoutMillis: Long = conf.get(PRODUCER_CACHE_TIMEOUT)

  @GuardedBy("this")
  private val cache = new mutable.HashMap[CacheKey, CachedProducerEntry]

  private def startEvictorThread(): Option[ScheduledFuture[_]] = {
    val evictorThreadRunIntervalMillis = conf.get(PRODUCER_CACHE_EVICTOR_THREAD_RUN_INTERVAL)
    if (evictorThreadRunIntervalMillis > 0) {
      val future = executorService.scheduleAtFixedRate(() => {
        Utils.tryLogNonFatalError(evictExpired())
      }, 0, evictorThreadRunIntervalMillis, TimeUnit.MILLISECONDS)
      Some(future)
    } else {
      None
    }
  }

  private val scheduled = startEvictorThread()

  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  private[producer] def acquire(kafkaParams: ju.Map[String, Object]): CachedKafkaProducer = {
    val updatedKafkaProducerConfiguration =
      KafkaConfigUpdater("executor", kafkaParams.asScala.toMap)
        .setAuthenticationConfigIfNeeded()
        .build()
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(updatedKafkaProducerConfiguration)
    synchronized {
      val entry = cache.getOrElseUpdate(paramsSeq, {
        val producer = createKafkaProducer(paramsSeq)
        val cachedProducer = new CachedKafkaProducer(paramsSeq, producer)
        new CachedProducerEntry(cachedProducer,
          TimeUnit.MILLISECONDS.toNanos(cacheExpireTimeoutMillis))
      })
      entry.handleBorrowed()
      entry.producer
    }
  }

  private[producer] def release(producer: CachedKafkaProducer): Unit = {
    synchronized {
      cache.get(producer.cacheKey) match {
        case Some(entry) if entry.producer.id == producer.id =>
          entry.handleReturned(clock.nanoTime())
        case _ =>
          logWarning(s"Released producer ${producer.id} is not a member of the cache. Closing.")
          producer.close()
      }
    }
  }

  private[producer] def shutdown(): Unit = {
    scheduled.foreach(_.cancel(false))
    ThreadUtils.shutdown(executorService)
  }

  /** exposed for testing. */
  private[producer] def reset(): Unit = synchronized {
    cache.foreach { case (_, v) => v.producer.close() }
    cache.clear()
  }

  /** exposed for testing */
  private[producer] def getAsMap: Map[CacheKey, CachedProducerEntry] = cache.toMap

  private def evictExpired(): Unit = {
    val curTimeNs = clock.nanoTime()
    val producers = new mutable.ArrayBuffer[CachedProducerEntry]()
    synchronized {
      cache.retain { case (_, v) =>
        if (v.expired(curTimeNs)) {
          producers += v
          false
        } else {
          true
        }
      }
    }
    producers.foreach { _.producer.close() }
  }

  private def createKafkaProducer(paramsSeq: Seq[(String, Object)]): Producer = {
    val kafkaProducer: Producer = new Producer(paramsSeq.toMap.asJava)
    if (log.isDebugEnabled()) {
      val redactedParamsSeq = KafkaRedactionUtil.redactParams(paramsSeq)
      logDebug(s"Created a new instance of KafkaProducer for $redactedParamsSeq.")
    }
    kafkaProducer
  }

  private def paramsToSeq(kafkaParams: ju.Map[String, Object]): Seq[(String, Object)] = {
    kafkaParams.asScala.toSeq.sortBy(x => x._1)
  }
}

private[kafka010] object InternalKafkaProducerPool extends Logging {
  private val pool = new InternalKafkaProducerPool(
    Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf()))

  private type CacheKey = Seq[(String, Object)]
  private type Producer = KafkaProducer[Array[Byte], Array[Byte]]

  ShutdownHookManager.addShutdownHook { () =>
    try {
      pool.shutdown()
    } catch {
      case e: Throwable =>
        logWarning("Ignoring Exception while shutting down pools from shutdown hook", e)
    }
  }

  /**
   * This class is used as metadata of producer pool, and shouldn't be exposed to the public.
   * This class assumes thread-safety is guaranteed by the caller.
   */
  private[producer] class CachedProducerEntry(
      val producer: CachedKafkaProducer,
      cacheExpireTimeoutNs: Long) {
    private var _refCount: Long = 0L
    private var _expireAt: Long = Long.MaxValue

    /** exposed for testing */
    private[producer] def refCount: Long = _refCount
    private[producer] def expireAt: Long = _expireAt

    def handleBorrowed(): Unit = {
      _refCount += 1
      _expireAt = Long.MaxValue
    }

    def handleReturned(curTimeNs: Long): Unit = {
      require(_refCount > 0, "Reference count shouldn't become negative. Returning same producer " +
        "multiple times would occur this bug. Check the logic around returning producer.")

      _refCount -= 1
      if (_refCount == 0) {
        _expireAt = curTimeNs + cacheExpireTimeoutNs
      }
    }

    def expired(curTimeNs: Long): Boolean = _refCount == 0 && _expireAt < curTimeNs
  }

  def acquire(kafkaParams: ju.Map[String, Object]): CachedKafkaProducer = {
    pool.acquire(kafkaParams)
  }

  def release(producer: CachedKafkaProducer): Unit = {
    pool.release(producer)
  }

  def reset(): Unit = pool.reset()
}
