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
import java.util.concurrent.{ConcurrentMap, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import com.google.common.cache._
import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

private[kafka010] object CachedKafkaProducer extends Logging {

  private type Producer = KafkaProducer[Array[Byte], Array[Byte]]

  private val cacheExpireTimeout: Long = System.getProperty("spark.kafka.guava.cache.timeout",
    "10").toLong

  private val noneReturningLoader = new CacheLoader[String, Option[Producer]] {
    override def load(key: String): Option[Producer] = {
      None
    }
  }

  private val removalListener = new RemovalListener[String, Option[Producer]]() {
    override def onRemoval(notification: RemovalNotification[String, Option[Producer]]): Unit = {
      val uid: String = notification.getKey
      val producer = notification.getValue
      log.debug(s"Evicting kafka producer $producer uid:$uid, due to ${notification.getCause}")
      producer.foreach(x => close(uid, x))
    }
  }

  private val guavaCache: Cache[String, Option[Producer]] = CacheBuilder.newBuilder()
    .recordStats()
    .expireAfterAccess(cacheExpireTimeout, TimeUnit.MINUTES)
    .removalListener(removalListener)
    .build[String, Option[Producer]](noneReturningLoader)

  private def createKafkaProducer(
    producerConfiguration: ju.Map[String, Object]): Producer = {
    val uid = getUniqueId(producerConfiguration)
    val kafkaProducer: Producer = new Producer(producerConfiguration)
    guavaCache.put(uid.toString, Some(kafkaProducer))
    log.debug(s"Created a new instance of KafkaProducer for $producerConfiguration.")
    kafkaProducer
  }

  private def getUniqueId(kafkaParams: ju.Map[String, Object]): String = {
    val uid = kafkaParams.get(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId)
    assert(uid != null, s"KafkaParams($kafkaParams) not canonicalized.")
    uid.toString
  }

  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  private[kafka010] def getOrCreate(
    kafkaParams: ju.Map[String, Object]): Producer = synchronized {
    val params = if (!CanonicalizeKafkaParams.isCanonicalized(kafkaParams)) {
      CanonicalizeKafkaParams.computeUniqueCanonicalForm(kafkaParams)
    } else {
      kafkaParams
    }
    val uid = getUniqueId(params)
    Option(guavaCache.getIfPresent(uid)).flatten.getOrElse(createKafkaProducer(params))
  }

  /** For explicitly closing kafka producer */
  private[kafka010] def close(kafkaParams: ju.Map[String, Object]): Unit = {
    val params = if (!CanonicalizeKafkaParams.isCanonicalized(kafkaParams)) {
      CanonicalizeKafkaParams.computeUniqueCanonicalForm(kafkaParams)
    } else kafkaParams
    val uid = getUniqueId(params)

    guavaCache.invalidate(uid)
  }

  /** Auto close on cache evict */
  private def close(uid: String, producer: Producer): Unit = {
    try {
      val outcome = CanonicalizeKafkaParams.remove(
        new ju.HashMap[String, Object](
          Map(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId -> uid).asJava))
      logDebug(s"Removed kafka params from cache: $outcome.")
      logInfo(s"Closing the KafkaProducer with uid $uid.")
      producer.close()
    } catch {
      case NonFatal(e) => log.warn("Error while closing kafka producer.", e)
    }
  }

  // Intended for testing purpose only.
  private def clear(): Unit = {
    guavaCache.invalidateAll()
  }

  private def getAsMap: ConcurrentMap[String, Option[Producer]] = guavaCache.asMap()
}

/**
 * Canonicalize kafka params i.e. append a unique internal id to kafka params, if it already does
 * not exist. This is done to ensure, we have only one set of kafka parameters associated with a
 * unique ID.
 */
private[kafka010] object CanonicalizeKafkaParams extends Logging {

  @GuardedBy("this")
  private val registryMap = mutable.HashMap[String, String]()

  private[kafka010] val sparkKafkaParamsUniqueId: String =
    "spark.internal.sql.kafka.params.uniqueId"

  private def generateRandomUUID(kafkaParams: String): String = {
    val uuid = ju.UUID.randomUUID().toString
    logDebug(s"Generating a new unique id: $uuid for kafka params: $kafkaParams")
    registryMap.put(kafkaParams, uuid)
    uuid
  }

  private[kafka010] def isCanonicalized(kafkaParams: ju.Map[String, Object]): Boolean = {
    kafkaParams.get(sparkKafkaParamsUniqueId) != null
  }

  private[kafka010] def computeUniqueCanonicalForm(
    kafkaParams: ju.Map[String, Object]): ju.Map[String, Object] = synchronized {
    if (isCanonicalized(kafkaParams)) {
      logWarning(s"A unique id,$sparkKafkaParamsUniqueId ->" +
        s" ${kafkaParams.get(sparkKafkaParamsUniqueId)}" +
        s" already exists in kafka params, returning Kafka Params: $kafkaParams as is.")
     kafkaParams
    } else {
      val sortedMap = SortedMap.empty[String, Object] ++ kafkaParams.asScala
      val stringRepresentation: String = sortedMap.mkString("\n")
      val uuid =
        registryMap.getOrElse(stringRepresentation, generateRandomUUID(stringRepresentation))
      val newMap = new ju.HashMap[String, Object](kafkaParams)
      newMap.put(sparkKafkaParamsUniqueId, uuid)
      newMap
    }
  }

  private[kafka010] def remove(kafkaParams: ju.Map[String, Object]): Boolean = {
    val sortedMap = SortedMap.empty[String, Object] ++ kafkaParams.asScala
    val stringRepresentation: String = sortedMap.mkString("\n")
    registryMap.remove(stringRepresentation).isDefined
  }

  // For testing purpose only.
  private[kafka010] def clear(): Unit = {
    registryMap.clear()
  }
}
