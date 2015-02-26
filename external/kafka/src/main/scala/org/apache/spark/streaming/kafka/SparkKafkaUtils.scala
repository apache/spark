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
import kafka.utils.{ZkUtils, ZKGroupTopicDirs}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.Logging

private[streaming]
object SparkKafkaUtils extends Logging {

  /**
   * Commit the offset of Kafka's topic/partition, the commit mechanism follow Kafka 0.8.x's
   * metadata schema in Zookeeper.
   */
  def commitOffset(zkClient: ZkClient, groupId: String,
      offsetMap: Map[TopicAndPartition, Long]): Unit = {
    if (zkClient == null) {
      throw new IllegalStateException("Zookeeper client is unexpectedly null")
    }

    for ((topicAndPart, offset) <- offsetMap) {
      try {
        val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPart.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${topicAndPart.partition}"

        ZkUtils.updatePersistentPath(zkClient, zkPath, offset.toString)
      } catch {
        case e: Exception =>
          logWarning(s"Exception during commit offset $offset for topic" +
            s"${topicAndPart.topic}, partition ${topicAndPart.partition}", e)
      }

      logInfo(s"Committed offset $offset for topic ${topicAndPart.topic}, " +
        s"partition ${topicAndPart.partition}")
    }
  }
}
