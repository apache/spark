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

import scala.collection.JavaConverters;

import org.apache.kafka.common.TopicPartition;

import org.junit.Assert;
import org.junit.Test;

public class JavaLocationStrategySuite implements Serializable {

  @Test
  public void testLocationStrategyConstructors() {
    final String topic1 = "topic1";
    final TopicPartition tp1 = new TopicPartition(topic1, 0);
    final TopicPartition tp2 = new TopicPartition(topic1, 1);
    final Map<TopicPartition, String> hosts = new HashMap<>();
    hosts.put(tp1, "node1");
    hosts.put(tp2, "node2");
    final scala.collection.Map<TopicPartition, String> sHosts =
      JavaConverters.mapAsScalaMapConverter(hosts).asScala();

    // make sure constructors can be called from java
    final LocationStrategy c1 = PreferConsistent.create();
    final LocationStrategy c2 = PreferConsistent$.MODULE$;
    Assert.assertEquals(c1, c2);

    final LocationStrategy c3 = PreferBrokers.create();
    final LocationStrategy c4 = PreferBrokers$.MODULE$;
    Assert.assertEquals(c3, c4);

    final LocationStrategy c5 = PreferFixed.create(hosts);
    final LocationStrategy c6 = PreferFixed.apply(sHosts);
    Assert.assertEquals(c5, c6);

  }

}
