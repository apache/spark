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

import java.{util => ju}

import org.apache.kafka.common.TopicPartition

/**
 * Use this only if your executors are on the same nodes as your Kafka brokers.
 */
private[kafka010] case object PreferBrokers extends LocationStrategy

/**
 * Use this in most cases, it will consistently distribute partitions across all executors.
 */
private[kafka010] case object PreferConsistent extends LocationStrategy

/**
 * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
 * Any TopicPartition not specified in the map will use a consistent location.
 */
private[kafka010] case class PreferFixed(hostMap: ju.Map[TopicPartition, String])
  extends LocationStrategy
