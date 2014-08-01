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

package org.apache.spark.streaming.kafka;

import java.util.HashMap;

import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.junit.Test;
import com.google.common.collect.Maps;
import kafka.serializer.StringDecoder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;

public class JavaKafkaStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testKafkaStream() {
    HashMap<String, Integer> topics = Maps.newHashMap();

    // tests the API, does not actually test data receiving
    JavaPairReceiverInputDStream<String, String> test1 =
            KafkaUtils.createStream(ssc, "localhost:12345", "group", topics);
    JavaPairReceiverInputDStream<String, String> test2 = KafkaUtils.createStream(ssc, "localhost:12345", "group", topics,
      StorageLevel.MEMORY_AND_DISK_SER_2());

    HashMap<String, String> kafkaParams = Maps.newHashMap();
    kafkaParams.put("zookeeper.connect", "localhost:12345");
    kafkaParams.put("group.id","consumer-group");
      JavaPairReceiverInputDStream<String, String> test3 = KafkaUtils.createStream(ssc,
      String.class, String.class, StringDecoder.class, StringDecoder.class,
      kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2());
  }
}
