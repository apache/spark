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

package org.apache.spark.streaming.kafka010;

import java.io.Serializable;
import java.util.*;

import scala.Function1;
import scala.Long;
import scala.collection.JavaConversions;

import org.apache.kafka.common.TopicPartition;

import org.junit.Assert;
import org.junit.Test;

public class JavaConsumerStrategySuite implements Serializable {

  @Test
  public void testConsumerStrategyConstructors() {
    final String topic1 = "topic1";
    final Collection<String> topics = Arrays.asList(topic1);
    final scala.collection.Iterable<String> sTopics =
        JavaConversions.collectionAsScalaIterable(topics);
    final TopicPartition tp1 = new TopicPartition(topic1, 0);
    final TopicPartition tp2 = new TopicPartition(topic1, 1);
    final Collection<TopicPartition> parts = Arrays.asList(tp1, tp2);
    final scala.collection.Iterable<TopicPartition> sParts =
        JavaConversions.collectionAsScalaIterable(parts);
    final Map<String, Object> kafkaParams = new HashMap<String, Object>();
    kafkaParams.put("bootstrap.servers", "not used");
    final scala.collection.Map<String, Object> sKafkaParams =
        JavaConversions.mapAsScalaMap(kafkaParams);
    final Map<TopicPartition, java.lang.Long> offsets = new HashMap<>();
    offsets.put(tp1, 23L);

    final Map<TopicPartition, scala.Long> _sOffsets = new HashMap<>();
    offsets.put(tp1, 23L);

    final scala.collection.Map<TopicPartition, scala.Long> sOffsets =
      JavaConversions.<TopicPartition, scala.Long>mapAsScalaMap(_sOffsets);

    // make sure constructors can be called from java
    final ConsumerStrategy<String, String> sub1 =
      ConsumerStrategy.<String, String>Subscribe(sTopics, sKafkaParams, sOffsets);
    final ConsumerStrategy<String, String> sub2 =
        ConsumerStrategy.<String, String>Subscribe(sTopics, sKafkaParams);
    final ConsumerStrategy<String, String> sub3 =
        ConsumerStrategy.<String, String>Subscribe(topics, kafkaParams, offsets);
    final ConsumerStrategy<String, String> sub4 =
        ConsumerStrategy.<String, String>Subscribe(topics, kafkaParams);

    Assert.assertEquals(
      sub1.executorKafkaParams().get("bootstrap.servers"),
      sub3.executorKafkaParams().get("bootstrap.servers"));

    final ConsumerStrategy<String, String> asn1 =
        ConsumerStrategy.<String, String>Assign(sParts, sKafkaParams, sOffsets);
    final ConsumerStrategy<String, String> asn2 =
        ConsumerStrategy.<String, String>Assign(sParts, sKafkaParams);
    final ConsumerStrategy<String, String> asn3 =
        ConsumerStrategy.<String, String>Assign(parts, kafkaParams, offsets);
    final ConsumerStrategy<String, String> asn4 =
        ConsumerStrategy.<String, String>Assign(parts, kafkaParams);

    Assert.assertEquals(
      asn1.executorKafkaParams().get("bootstrap.servers"),
      asn3.executorKafkaParams().get("bootstrap.servers"));
  }
}
