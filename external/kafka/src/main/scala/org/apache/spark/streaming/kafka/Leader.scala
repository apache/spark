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

import kafka.common.TopicAndPartition

/** Host info for the leader of a Kafka TopicAndPartition */
final class Leader private(
    /** kafka topic name */
    val topic: String,
    /** kafka partition id */
    val partition: Int,
    /** kafka hostname */
    val host: String,
    /** kafka host's port */
    val port: Int) extends Serializable

object Leader {
  def create(topic: String, partition: Int, host: String, port: Int): Leader =
    new Leader(topic, partition, host, port)

  def create(topicAndPartition: TopicAndPartition, host: String, port: Int): Leader =
    new Leader(topicAndPartition.topic, topicAndPartition.partition, host, port)

  def apply(topic: String, partition: Int, host: String, port: Int): Leader =
    new Leader(topic, partition, host, port)

  def apply(topicAndPartition: TopicAndPartition, host: String, port: Int): Leader =
    new Leader(topicAndPartition.topic, topicAndPartition.partition, host, port)

}
