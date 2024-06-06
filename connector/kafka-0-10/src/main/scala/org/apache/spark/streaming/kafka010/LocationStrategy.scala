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

package org.apache.spark.streaming.kafka010

import java.{ util => ju }

import scala.jdk.CollectionConverters._

import org.apache.kafka.common.TopicPartition


/**
 * Choice of how to schedule consumers for a given TopicPartition on an executor.
 * See [[LocationStrategies]] to obtain instances.
 * Kafka 0.10 consumers prefetch messages, so it's important for performance
 * to keep cached consumers on appropriate executors, not recreate them for every partition.
 * Choice of location is only a preference, not an absolute; partitions may be scheduled elsewhere.
 */
sealed abstract class LocationStrategy

private case object PreferBrokers extends LocationStrategy

private case object PreferConsistent extends LocationStrategy

private case class PreferFixed(hostMap: ju.Map[TopicPartition, String]) extends LocationStrategy

/**
 * Object to obtain instances of [[LocationStrategy]]
 */
object LocationStrategies {
  /**
   * Use this only if your executors are on the same nodes as your Kafka brokers.
   */
  def PreferBrokers: LocationStrategy =
    org.apache.spark.streaming.kafka010.PreferBrokers

  /**
   * Use this in most cases, it will consistently distribute partitions across all executors.
   */
  def PreferConsistent: LocationStrategy =
    org.apache.spark.streaming.kafka010.PreferConsistent

  /**
   * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
   * Any TopicPartition not specified in the map will use a consistent location.
   */
  def PreferFixed(hostMap: collection.Map[TopicPartition, String]): LocationStrategy =
    new PreferFixed(new ju.HashMap[TopicPartition, String](hostMap.asJava))

  /**
   * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
   * Any TopicPartition not specified in the map will use a consistent location.
   */
  def PreferFixed(hostMap: ju.Map[TopicPartition, String]): LocationStrategy =
    new PreferFixed(hostMap)
}
