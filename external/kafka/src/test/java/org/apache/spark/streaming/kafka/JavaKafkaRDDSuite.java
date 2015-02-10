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
import java.util.Arrays;

import org.apache.spark.SparkConf;

import scala.Tuple2;

import junit.framework.Assert;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;

public class JavaKafkaRDDSuite implements Serializable {
  private transient JavaSparkContext sc = null;
  private transient KafkaStreamSuiteBase suiteBase = null;

  @Before
  public void setUp() {
    suiteBase = new KafkaStreamSuiteBase() { };
    suiteBase.setupKafka();
    System.clearProperty("spark.driver.port");
    SparkConf sparkConf = new SparkConf()
      .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
    sc = new JavaSparkContext(sparkConf);
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
    System.clearProperty("spark.driver.port");
    suiteBase.tearDownKafka();
  }

  @Test
  public void testKafkaRDD() throws InterruptedException {
    String topic1 = "topic1";
    String topic2 = "topic2";

    String[] topic1data = createTopicAndSendData(topic1);
    String[] topic2data = createTopicAndSendData(topic2);

    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", suiteBase.brokerAddress());

    OffsetRange[] offsetRanges = {
      OffsetRange.create(topic1, 0, 0, 1),
      OffsetRange.create(topic2, 0, 0, 1)
    };

    HashMap<TopicAndPartition, Broker> emptyLeaders = new HashMap();
    HashMap<TopicAndPartition, Broker> leaders = new HashMap();
    String[] hostAndPort = suiteBase.brokerAddress().split(":");
    Broker broker = Broker.create(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    leaders.put(new TopicAndPartition(topic1, 0), broker);
    leaders.put(new TopicAndPartition(topic2, 0), broker);

    JavaRDD<String> rdd1 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        offsetRanges
    ).map(
        new Function<Tuple2<String, String>, String>() {
          @Override
          public String call(scala.Tuple2<String, String> kv) throws Exception {
            return kv._2();
          }
        }
    );

    JavaRDD<String> rdd2 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        String.class,
        kafkaParams,
        offsetRanges,
        emptyLeaders,
        new Function<MessageAndMetadata<String, String>, String>() {
          @Override
          public String call(MessageAndMetadata<String, String> msgAndMd) throws Exception {
            return msgAndMd.message();
          }
        }
    );

    JavaRDD<String> rdd3 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        String.class,
        kafkaParams,
        offsetRanges,
        leaders,
        new Function<MessageAndMetadata<String, String>, String>() {
          @Override
          public String call(MessageAndMetadata<String, String> msgAndMd) throws Exception {
            return msgAndMd.message();
          }
        }
    );

    // just making sure the java user apis work; the scala tests handle logic corner cases
    long count1 = rdd1.count();
    long count2 = rdd2.count();
    long count3 = rdd3.count();
    Assert.assertTrue(count1 > 0);
    Assert.assertEquals(count1, count2);
    Assert.assertEquals(count1, count3);
  }

  private  String[] createTopicAndSendData(String topic) {
    String[] data = { topic + "-1", topic + "-2", topic + "-3"};
    suiteBase.createTopic(topic);
    suiteBase.sendMessages(topic, data);
    return data;
  }
}
