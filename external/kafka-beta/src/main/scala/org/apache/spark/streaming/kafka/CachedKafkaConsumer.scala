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

package org.apache.spark.streaming.kafka

import java.{ util => ju }

import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{ Logging, SparkConf }

/** Consumer of single topicpartition, intended for cached reuse.
  * Underlying consumer is not threadsafe, so neither is this,
  * but processing the same topicpartition and group id in multiple threads would be bad anyway.
  */
private[kafka]
class CachedKafkaConsumer[K, V] private(
  val groupId: String,
  val topic: String,
  val partition: Int,
  val kafkaParams: ju.Map[String, Object]) extends Logging {

  assert(groupId == kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG),
    "groupId used for cache key must match the groupId in kafkaParams")

  val topicPartition = new TopicPartition(topic, partition)

  protected val consumer = {
    val c = new KafkaConsumer[K, V](kafkaParams)
    val tps = new ju.ArrayList[TopicPartition]()
    tps.add(topicPartition)
    c.assign(tps)
    c
  }

  protected var buffer = ju.Collections.emptyList[ConsumerRecord[K, V]]().iterator

  /** Get the record for the given offset, waiting up to timeout ms if IO is necessary.
    * Sequential forward access will use buffers, but random access will be horribly inefficient.
    */
  def get(offset: Long, timeout: Long): ConsumerRecord[K, V] = {
    if (!buffer.hasNext) { poll(timeout) }

    var record = buffer.next()

    if (record.offset != offset) {
      log.info(s"buffer miss for $groupId $topic $partition $offset")
      seek(offset)
      poll(timeout)
      record = buffer.next()
      assert(record.offset == offset,
        s"Failed to get offset $offset after seeking to it")
    }

    record
  }

  private def seek(offset: Long): Unit = {
    consumer.seek(topicPartition, offset)
  }

  private def poll(timeout: Long): Unit = {
    buffer = consumer.poll(timeout).records(topicPartition).iterator
  }

}

private[kafka]
object CachedKafkaConsumer extends Logging {

  private case class CacheKey(groupId: String, topic: String, partition: Int)

  // Don't want to depend on guava, don't want a cleanup thread, use a simple LinkedHashMap
  private var cache: ju.LinkedHashMap[CacheKey, CachedKafkaConsumer[_, _]] = null

  /** Must be called before get, once per JVM, to configure the cache. Further calls are ignored */
  def init(conf: SparkConf): Unit = CachedKafkaConsumer.synchronized {
    if (null == cache) {
      val initial = conf.getInt("spark.streaming.kafka.consumer.cache.initialCapacity", 16)
      val max = conf.getInt("spark.streaming.kafka.consumer.cache.maxCapacity", 64)
      val load = conf.getDouble("spark.streaming.kafka.consumer.cache.loadFactor", 0.75).toFloat
      cache = new ju.LinkedHashMap[CacheKey, CachedKafkaConsumer[_, _]](initial, load, true) {
        override def removeEldestEntry(
          entry: ju.Map.Entry[CacheKey, CachedKafkaConsumer[_, _]]): Boolean = {
          if (this.size > max) {
            entry.getValue.consumer.close()
            true
          } else {
            false
          }
        }
      }
    }
  }

  /** Get a cached consumer for groupId, assigned to topic and partition.
    * If matching consumer doesn't already exist, will be created using kafkaParams.
    */
  def get[K, V](
    groupId: String,
    topic: String,
    partition: Int,
    kafkaParams: ju.Map[String, Object]): CachedKafkaConsumer[K, V] =
    CachedKafkaConsumer.synchronized {
      val k = CacheKey(groupId, topic, partition)
      val v = cache.get(k)
      if (null == v) {
        log.info(s"cache miss for $groupId $topic $partition")
        val c = new CachedKafkaConsumer[K, V](groupId, topic, partition, kafkaParams)
        cache.put(k, c)
        c
      } else {
        // any given topicpartition should have a consistent key and value type
        v.asInstanceOf[CachedKafkaConsumer[K, V]]
      }
    }

  /** remove consumer for given groupId, topic, and partition, if it exists */
  def remove(groupId: String, topic: String, partition: Int): Unit =
    CachedKafkaConsumer.synchronized {
      val k = CacheKey(groupId, topic, partition)
      val v = cache.get(k)
      if (null != v) {
        v.consumer.close()
        cache.remove(k)
      }
    }
}
