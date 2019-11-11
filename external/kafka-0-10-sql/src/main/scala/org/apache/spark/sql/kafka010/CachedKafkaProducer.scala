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
import java.io.Closeable

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.{KafkaConfigUpdater, KafkaRedactionUtil}
import org.apache.spark.sql.kafka010.InternalKafkaProducerPool._
import org.apache.spark.util.ShutdownHookManager

private[kafka010] class CachedKafkaProducer(val kafkaParams: ju.Map[String, Object])
  extends Closeable with Logging {

  private type Producer = KafkaProducer[Array[Byte], Array[Byte]]

  private val producer = createProducer()

  private def createProducer(): Producer = {
    val producer: Producer = new Producer(kafkaParams)
    if (log.isDebugEnabled()) {
      val redactedParamsSeq = KafkaRedactionUtil.redactParams(toCacheKey(kafkaParams))
      logDebug(s"Created a new instance of kafka producer for $redactedParamsSeq.")
    }
    producer
  }

  override def close(): Unit = {
    try {
      if (log.isInfoEnabled()) {
        val redactedParamsSeq = KafkaRedactionUtil.redactParams(toCacheKey(kafkaParams))
        logInfo(s"Closing the KafkaProducer with params: ${redactedParamsSeq.mkString("\n")}.")
      }
      producer.close()
    } catch {
      case NonFatal(e) => logWarning("Error while closing kafka producer.", e)
    }
  }

  def send(record: ProducerRecord[Array[Byte], Array[Byte]], callback: Callback): Unit = {
    producer.send(record, callback)
  }

  def flush(): Unit = {
    producer.flush()
  }
}

private[kafka010] object CachedKafkaProducer extends Logging {

  private val sparkConf = SparkEnv.get.conf
  private val producerPool = new InternalKafkaProducerPool(sparkConf)

  ShutdownHookManager.addShutdownHook { () =>
    try {
      producerPool.close()
    } catch {
      case e: Throwable =>
        logWarning("Ignoring exception while shutting down pool from shutdown hook", e)
    }
  }

  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  def acquire(kafkaParams: ju.Map[String, Object]): CachedKafkaProducer = {
    val updatedKafkaParams =
      KafkaConfigUpdater("executor", kafkaParams.asScala.toMap)
        .setAuthenticationConfigIfNeeded()
        .build()
    val key = toCacheKey(updatedKafkaParams)
    producerPool.borrowObject(key, updatedKafkaParams)
  }

  def release(producer: CachedKafkaProducer): Unit = {
    producerPool.returnObject(producer)
  }

  private[kafka010] def clear(): Unit = {
    producerPool.reset()
  }
}
