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

import scala.jdk.javaapi.CollectionConverters;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JavaLocationStrategySuite implements Serializable {

  @Test
  public void testLocationStrategyConstructors() {
    final String topic1 = "topic1";
    final TopicPartition tp1 = new TopicPartition(topic1, 0);
    final TopicPartition tp2 = new TopicPartition(topic1, 1);
    final Map<TopicPartition, String> hosts = new HashMap<>();
    hosts.put(tp1, "node1");
    hosts.put(tp2, "node2");
    final scala.collection.Map<TopicPartition, String> sHosts = CollectionConverters.asScala(hosts);

    // make sure constructors can be called from java
    final LocationStrategy c1 = LocationStrategies.PreferConsistent();
    final LocationStrategy c2 = LocationStrategies.PreferConsistent();
    Assertions.assertSame(c1, c2);

    final LocationStrategy c3 = LocationStrategies.PreferBrokers();
    final LocationStrategy c4 = LocationStrategies.PreferBrokers();
    Assertions.assertSame(c3, c4);

    Assertions.assertNotSame(c1, c3);

    final LocationStrategy c5 = LocationStrategies.PreferFixed(hosts);
    final LocationStrategy c6 = LocationStrategies.PreferFixed(sHosts);
    Assertions.assertEquals(c5, c6);
  }

}
