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

import scala.collection.immutable.SortedMap
import scala.collection.mutable

import org.apache.kafka.clients.producer.KafkaProducer

import org.apache.spark.internal.Logging

private[kafka010] object CachedKafkaProducer extends Logging {

  type Producer = KafkaProducer[Array[Byte], Array[Byte]]

  private val cacheMap = new mutable.HashMap[String, Producer]()

  private def createKafkaProducer(
    producerConfiguration: ju.Map[String, Object]): Producer = {
    val uid = producerConfiguration.get(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId)
    val kafkaProducer: Producer = new Producer(producerConfiguration)
    cacheMap.put(uid.toString, kafkaProducer)
    log.debug(s"Created a new instance of KafkaProducer for $producerConfiguration.")
    kafkaProducer
  }

  private def getUniqueId(kafkaParams: ju.Map[String, Object]): String = {
    val uid = kafkaParams.get(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId)
    assert(uid != null, s"KafkaParams($kafkaParams) not canonicalized, see API doc on " +
      "`CanonicalizeKafkaParams`")
    uid.toString
  }

  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  def getOrCreate(
    kafkaParams: ju.Map[String, Object]): Producer = synchronized {
    val params = if (!CanonicalizeKafkaParams.isCanonicalized(kafkaParams)) {
      CanonicalizeKafkaParams.computeUniqueCanonicalForm(kafkaParams)
    } else kafkaParams
    val uid = getUniqueId(params)
    cacheMap.getOrElse(uid.toString, createKafkaProducer(params))
  }

  def close(kafkaParams: ju.Map[String, Object]): Unit = {
    val uid = getUniqueId(kafkaParams)
    val producer: Option[Producer] =
      cacheMap.remove(uid)

    if (producer.isDefined) {
      producer.foreach(_.close())
    } else {
      log.warn(s"No KafkaProducer found in cache for $kafkaParams.")
    }
  }

  // Intended for testing purpose only.
  private def clear(): Unit = {
    cacheMap.foreach(x => x._2.close())
    cacheMap.clear()
  }
}

/**
 * Canonicalize kafka params i.e. append a unique internal id to kafka params, if it already does
 * not exist. This is done to ensure, we have only one set of kafka parameters associated with a
 * unique ID.
 */
private[kafka010] object CanonicalizeKafkaParams extends Logging {

  import scala.collection.JavaConverters._

  private val registryMap = mutable.HashMap[String, String]()

  private[kafka010] val sparkKafkaParamsUniqueId: String =
    "spark.internal.sql.kafka.params.uniqueId"

  private def generateRandomUUID(kafkaParams: String): String = {
    val uuid = ju.UUID.randomUUID().toString
    logDebug(s"Generating a new unique id:$uuid for kafka params: $kafkaParams")
    registryMap.put(kafkaParams, uuid)
    uuid
  }

  private[kafka010] def isCanonicalized(kafkaParams: ju.Map[String, Object]): Boolean = {
    if (kafkaParams.get(sparkKafkaParamsUniqueId) != null) {
      true
    } else {
      false
    }
  }

  private[kafka010] def computeUniqueCanonicalForm(
    kafkaParams: ju.Map[String, Object]): ju.Map[String, Object] = synchronized {
    if (kafkaParams.get(sparkKafkaParamsUniqueId) != null) {
      logWarning(s"A unique id,$sparkKafkaParamsUniqueId ->" +
        s" ${kafkaParams.get(sparkKafkaParamsUniqueId)}" +
        s" already exists in kafka params, returning Kafka Params:$kafkaParams as is.")
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

  // For testing purpose only.
  private[kafka010] def clear(): Unit = {
    registryMap.clear()
  }

}
