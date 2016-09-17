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

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Interface for user-supplied configurations that can't otherwise be set via Spark properties,
 * because they need tweaking on a per-partition basis,
 */
@Experimental
trait PerPartitionConfig {
  /**
   *  Maximum rate (number of records per second) at which data will be read
   *  from each Kafka partition.
   */
  def maxRatePerPartition(topicPartition: TopicPartition): Int
}

/**
 * Default per-partition configuration
 */
private class DefaultPerPartitionConfig(conf: SparkConf)
    extends PerPartitionConfig with Serializable {
  val maxRate = conf.getInt("spark.streaming.kafka.maxRatePerPartition", 0)

  def maxRatePerPartition(topicPartition: TopicPartition): Int = maxRate
}
