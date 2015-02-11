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

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Arrays;

import org.apache.spark.SparkConf;

import scala.Tuple2;

import junit.framework.Assert;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;

public class JavaDirectKafkaStreamSuite implements Serializable {
  private transient JavaStreamingContext ssc = null;
  private transient Random random = new Random();
  private transient KafkaStreamSuiteBase suiteBase = null;

  @Before
  public void setUp() {
      suiteBase = new KafkaStreamSuiteBase() { };
      suiteBase.setupKafka();
      System.clearProperty("spark.driver.port");
      SparkConf sparkConf = new SparkConf()
              .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
      ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(200));
  }

  @After
  public void tearDown() {
      ssc.stop();
      ssc = null;
      System.clearProperty("spark.driver.port");
      suiteBase.tearDownKafka();
  }

  @Test
  public void testKafkaStream() throws InterruptedException {
    String topic1 = "topic1";
    String topic2 = "topic2";

    String[] topic1data = createTopicAndSendData(topic1);
    String[] topic2data = createTopicAndSendData(topic2);

    HashSet<String> sent = new HashSet<String>();
    sent.addAll(Arrays.asList(topic1data));
    sent.addAll(Arrays.asList(topic2data));

    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", suiteBase.brokerAddress());
    kafkaParams.put("auto.offset.reset", "smallest");

    JavaDStream<String> stream1 = KafkaUtils.createDirectStream(
        ssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicToSet(topic1)
    ).map(
        new Function<Tuple2<String, String>, String>() {
          @Override
          public String call(scala.Tuple2<String, String> kv) throws Exception {
            return kv._2();
          }
        }
    );

    JavaDStream<String> stream2 = KafkaUtils.createDirectStream(
        ssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        String.class,
        kafkaParams,
        topicOffsetToMap(topic2, (long) 0),
        new Function<MessageAndMetadata<String, String>, String>() {
          @Override
          public String call(MessageAndMetadata<String, String> msgAndMd) throws Exception {
            return msgAndMd.message();
          }
        }
    );
    JavaDStream<String> unifiedStream = stream1.union(stream2);

    final HashSet<String> result = new HashSet<String>();
    unifiedStream.foreachRDD(
        new Function<JavaRDD<String>, Void>() {
          @Override
          public Void call(org.apache.spark.api.java.JavaRDD<String> rdd) throws Exception {
            result.addAll(rdd.collect());
            return null;
          }
        }
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

  private HashSet<String> topicToSet(String topic) {
    HashSet<String> topicSet = new HashSet<String>();
    topicSet.add(topic);
    return topicSet;
  }

  private HashMap<TopicAndPartition, Long> topicOffsetToMap(String topic, Long offsetToStart) {
    HashMap<TopicAndPartition, Long> topicMap = new HashMap<TopicAndPartition, Long>();
    topicMap.put(new TopicAndPartition(topic, 0), offsetToStart);
    return topicMap;
  }

  private  String[] createTopicAndSendData(String topic) {
    String[] data = { topic + "-1", topic + "-2", topic + "-3"};
    suiteBase.createTopic(topic);
    suiteBase.sendMessages(topic, data);
    return data;
  }
}
