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

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import java.lang.{Integer => JInt}
import java.util.{Map => JMap}

import kafka.serializer.Decoder

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.{JavaStreamingContext, JavaPairDStream}

/**
 * Subclass of [[org.apache.spark.streaming.api.java.JavaStreamingContext]] that has extra
 * functions for creating Kafka input streams.
 */
class JavaStreamingContextWithKafka(javaStreamingContext: JavaStreamingContext)
  extends JavaStreamingContext(javaStreamingContext.ssc) {

  /**
   * Create an input stream that pulls messages form a Kafka Broker.
   * @param zkQuorum Zookeper quorum (hostname:port,hostname:port,..).
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   * in its own thread.
   */
  def kafkaStream(
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt]
    ): JavaPairDStream[String, String] = {
      implicit val cmt: ClassTag[String] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
      ssc.kafkaStream(zkQuorum, groupId, Map(topics.mapValues(_.intValue()).toSeq: _*))
  }

  /**
   * Create an input stream that pulls messages form a Kafka Broker.
   * @param zkQuorum Zookeper quorum (hostname:port,hostname:port,..).
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *               in its own thread.
   * @param storageLevel RDD storage level.
   *
   */
  def kafkaStream(
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairDStream[String, String] = {
    implicit val cmt: ClassTag[String] =
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    ssc.kafkaStream(zkQuorum, groupId, Map(topics.mapValues(_.intValue()).toSeq: _*), storageLevel)
  }

  /**
   * Create an input stream that pulls messages form a Kafka Broker.
   * @param keyTypeClass Key type of RDD
   * @param valueTypeClass value type of RDD
   * @param keyDecoderClass Type of kafka key decoder
   * @param valueDecoderClass Type of kafka value decoder
   * @param kafkaParams Map of kafka configuration paramaters.
   *                    See: http://kafka.apache.org/configuration.html
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   * in its own thread.
   * @param storageLevel RDD storage level. Defaults to memory-only
   */
  def kafkaStream[K, V, U <: Decoder[_], T <: Decoder[_]](
      keyTypeClass: Class[K],
      valueTypeClass: Class[V],
      keyDecoderClass: Class[U],
      valueDecoderClass: Class[T],
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] =
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[K]]
    implicit val valueCmt: ClassTag[V] =
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]

    implicit val keyCmd: Manifest[U] = implicitly[Manifest[AnyRef]].asInstanceOf[Manifest[U]]
    implicit val valueCmd: Manifest[T] = implicitly[Manifest[AnyRef]].asInstanceOf[Manifest[T]]

    ssc.kafkaStream[K, V, U, T](
      kafkaParams.toMap, Map(topics.mapValues(_.intValue()).toSeq: _*), storageLevel)
  }
}
