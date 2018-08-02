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

import org.apache.spark.SparkConf

private object DirectKafkaConf {

  def pollTimeout(conf: SparkConf): Long = conf
    .getLong("spark.streaming.kafka.consumer.poll.ms",
      conf.getTimeAsSeconds("spark.network.timeout", "120s") * 1000L)

  def cacheInitialCapacity(conf: SparkConf): Int = conf
    .getInt("spark.streaming.kafka.consumer.cache.initialCapacity", 16)

  def cacheMaxCapacity(conf: SparkConf): Int = conf
    .getInt("spark.streaming.kafka.consumer.cache.maxCapacity", 64)

  def cacheLoadFactor(conf: SparkConf): Float = conf
    .getDouble("spark.streaming.kafka.consumer.cache.loadFactor", 0.75).toFloat

  def initialRate(conf: SparkConf): Long = conf
    .getLong("spark.streaming.backpressure.initialRate", 0)

  def nonConsecutive(conf: SparkConf): Boolean = conf
    .getBoolean("spark.streaming.kafka.allowNonConsecutiveOffsets", false)

  def useConsumerCache(conf: SparkConf): Boolean = conf
    .getBoolean("spark.streaming.kafka.consumer.cache.enabled", true)
}
