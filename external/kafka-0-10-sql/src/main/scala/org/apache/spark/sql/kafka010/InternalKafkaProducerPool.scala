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

import scala.collection.JavaConverters._

import org.apache.commons.pool2.PooledObject

import org.apache.spark.SparkConf
import org.apache.spark.sql.kafka010.InternalKafkaProducerPool.CacheKey

private[kafka010] class InternalKafkaProducerPool(
    objectFactory: ProducerObjectFactory,
    poolConfig: ProducerPoolConfig)
  extends InternalKafkaConnectorPool[CacheKey, CachedKafkaProducer](
      objectFactory,
      poolConfig,
      new CustomSwallowedExceptionListener("producer")) {

  def this(conf: SparkConf) = {
    this(new ProducerObjectFactory, new ProducerPoolConfig(conf))
  }

  override protected def createKey(producer: CachedKafkaProducer): CacheKey = {
    InternalKafkaProducerPool.toCacheKey(producer.kafkaParams)
  }
}

private class ProducerPoolConfig(conf: SparkConf) extends PoolConfig[CachedKafkaProducer] {
  def softMaxSize: Int = conf.get(PRODUCER_CACHE_CAPACITY)
  def jmxEnabled: Boolean = conf.get(PRODUCER_CACHE_JMX_ENABLED)
  def minEvictableIdleTimeMillis: Long = conf.get(PRODUCER_CACHE_TIMEOUT)
  def evictorThreadRunIntervalMillis: Long = conf.get(PRODUCER_CACHE_EVICTOR_THREAD_RUN_INTERVAL)
  def jmxNamePrefix: String = "kafka010-cached-simple-kafka-producer-pool"
}

private class ProducerObjectFactory extends ObjectFactory[CacheKey, CachedKafkaProducer] {
  override protected def createValue(
      key: CacheKey,
      kafkaParams: ju.Map[String, Object]): CachedKafkaProducer = {
    new CachedKafkaProducer(kafkaParams)
  }
}

private[kafka010] object InternalKafkaProducerPool {
  type CacheKey = Seq[(String, Object)]

  def toCacheKey(params: ju.Map[String, Object]): CacheKey = {
    params.asScala.toSeq.sortBy(x => x._1)
  }
}
