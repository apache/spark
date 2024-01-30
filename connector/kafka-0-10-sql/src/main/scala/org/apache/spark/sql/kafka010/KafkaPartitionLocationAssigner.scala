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



import org.apache.kafka.common.PartitionInfo

import org.apache.spark.util.Utils

trait KafkaPartitionLocationAssigner {
  /**
  * Returns a map of what executors are eligible for each Kafka
  * partition. This map need not be exhaustive; if a given Kafka
  * partition is not associated with any executors, the partition
  * will be consistently assigned to any available executor.
  *
  * @param partInfos      Partition information per topic and partition
  *                       subscribed. This collection is provided sorted by
  *                       topic, then partition
  *
  * @param knownExecutors Executor addresses for this app. This
  *                       collection is provided sorted to aid in
  *                       consistently associating partitions with
  *                       executors
  */
  def getLocationPreferences(
    partInfos: Seq[PartitionInfo],
    knownExecutors: Seq[String]): Map[PartitionInfo, Seq[String]]
}

object KafkaPartitionLocationAssigner {
  def instance(maybeClassName: Option[String]): KafkaPartitionLocationAssigner =
    maybeClassName.map { className =>
      Utils.classForName[KafkaPartitionLocationAssigner](className)
        .getConstructor()
        .newInstance()
    }.getOrElse(DefaultKafkaPartitionLocationAssigner)
}

object DefaultKafkaPartitionLocationAssinger extends KafkaPartitionLocationAssigner {
  def getLocationPreferences(
    partInfos: Seq[PartitionInfo],
    knownExecutors: Seq[String]): Map[PartitionInfo, Seq[String]] =
    Map.empty
}

