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

import org.apache.commons.pool2.PooledObject

import org.apache.spark.SparkConf
import org.apache.spark.sql.kafka010.KafkaDataConsumer.CacheKey

// TODO: revisit the relation between CacheKey and kafkaParams - for now it looks a bit weird
//   as we force all consumers having same (groupId, topicPartition) to have same kafkaParams
//   which might be viable in performance perspective (kafkaParams might be too huge to use
//   as a part of key), but there might be the case kafkaParams could be different -
//   cache key should be differentiated for both kafkaParams.
private[kafka010] class InternalKafkaConsumerPool(
    objectFactory: ConsumerObjectFactory,
    poolConfig: ConsumerPoolConfig)
  extends InternalKafkaConnectorPool[CacheKey, InternalKafkaConsumer](
      objectFactory,
      poolConfig,
      new CustomSwallowedExceptionListener("consumer")) {

  def this(conf: SparkConf) = {
    this(new ConsumerObjectFactory, new ConsumerPoolConfig(conf))
  }

  override protected def createKey(consumer: InternalKafkaConsumer): CacheKey = {
    new CacheKey(consumer.topicPartition, consumer.kafkaParams)
  }
}

private class ConsumerPoolConfig(conf: SparkConf) extends PoolConfig[InternalKafkaConsumer] {
  def softMaxSize: Int = conf.get(CONSUMER_CACHE_CAPACITY)
  def jmxEnabled: Boolean = conf.get(CONSUMER_CACHE_JMX_ENABLED)
  def minEvictableIdleTimeMillis: Long = conf.get(CONSUMER_CACHE_TIMEOUT)
  def evictorThreadRunIntervalMillis: Long = conf.get(CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL)
  def jmxNamePrefix: String = "kafka010-cached-simple-kafka-consumer-pool"
}

private class ConsumerObjectFactory extends ObjectFactory[CacheKey, InternalKafkaConsumer] {
  override protected def createValue(
      key: CacheKey,
      kafkaParams: ju.Map[String, Object]): InternalKafkaConsumer = {
    new InternalKafkaConsumer(key.topicPartition, kafkaParams)
  }
}
