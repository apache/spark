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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaDirectKafkaStreamSuite implements Serializable {
  private transient JavaStreamingContext ssc = null;
  private transient KafkaTestUtils kafkaTestUtils = null;

  @Before
  public void setUp() {
    kafkaTestUtils = new KafkaTestUtils();
    kafkaTestUtils.setup();
    SparkConf sparkConf = new SparkConf()
      .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
    ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(200));
  }

  @After
  public void tearDown() {
    if (ssc != null) {
      ssc.stop();
      ssc = null;
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown();
      kafkaTestUtils = null;
    }
  }

  @Test
  public void testKafkaStream() throws InterruptedException {
    final String topic1 = "topic1";
    final String topic2 = "topic2";
    // hold a reference to the current offset ranges, so it can be used downstream
    final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

    String[] topic1data = createTopicAndSendData(topic1);
    String[] topic2data = createTopicAndSendData(topic2);

    Set<String> sent = new HashSet<>();
    sent.addAll(Arrays.asList(topic1data));
    sent.addAll(Arrays.asList(topic2data));

    Random random = new Random();

    final Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", kafkaTestUtils.brokerAddress());
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("group.id", "java-test-consumer-" + random.nextInt() +
      "-" + System.currentTimeMillis());

    JavaInputDStream<ConsumerRecord<String, String>> istream1 = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.Subscribe(Arrays.asList(topic1), kafkaParams)
    );

    JavaDStream<String> stream1 = istream1.transform(
      // Make sure you can get offset ranges from the rdd
      (Function<JavaRDD<ConsumerRecord<String, String>>,
        JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        offsetRanges.set(offsets);
        Assert.assertEquals(topic1, offsets[0].topic());
        return rdd;
      }
    ).map(
      (Function<ConsumerRecord<String, String>, String>) ConsumerRecord::value
    );

    final Map<String, Object> kafkaParams2 = new HashMap<>(kafkaParams);
    kafkaParams2.put("group.id", "java-test-consumer-" + random.nextInt() +
      "-" + System.currentTimeMillis());

    JavaInputDStream<ConsumerRecord<String, String>> istream2 = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.Subscribe(Arrays.asList(topic2), kafkaParams2)
    );

    JavaDStream<String> stream2 = istream2.transform(
      // Make sure you can get offset ranges from the rdd
      (Function<JavaRDD<ConsumerRecord<String, String>>,
        JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        offsetRanges.set(offsets);
        Assert.assertEquals(topic2, offsets[0].topic());
        return rdd;
      }
    ).map(
      (Function<ConsumerRecord<String, String>, String>) ConsumerRecord::value
    );

    JavaDStream<String> unifiedStream = stream1.union(stream2);

    final Set<String> result = Collections.synchronizedSet(new HashSet<>());
    unifiedStream.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> result.addAll(rdd.collect())
    );
    ssc.start();
    long startTime = System.currentTimeMillis();
    boolean matches = false;
    while (!matches && System.currentTimeMillis() - startTime < 20000) {
      matches = sent.size() == result.size();
      Thread.sleep(50);
    }
    Assert.assertEquals(sent, result);
    ssc.stop();
  }

  private  String[] createTopicAndSendData(String topic) {
    String[] data = { topic + "-1", topic + "-2", topic + "-3"};
    kafkaTestUtils.createTopic(topic);
    kafkaTestUtils.sendMessages(topic, data);
    return data;
  }
}
