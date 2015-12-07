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

package org.apache.spark.streaming.kafka.v09;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.kafka.v09.Broker;
import org.apache.spark.streaming.kafka.v09.KafkaTestUtils;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.apache.spark.streaming.kafka.v09.OffsetRange;
import scala.Tuple2;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class JavaKafkaRDDSuite implements Serializable {
  private transient JavaSparkContext sc = null;
  private transient KafkaTestUtils kafkaTestUtils = null;

  @Before
  public void setUp() {
    kafkaTestUtils = new KafkaTestUtils();
    kafkaTestUtils.setup();
    SparkConf sparkConf = new SparkConf()
        .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
    sc = new JavaSparkContext(sparkConf);
  }

  @After
  public void tearDown() {
    if (sc != null) {
      sc.stop();
      sc = null;
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown();
      kafkaTestUtils = null;
    }
  }

  @Test
  public void testKafkaRDD() throws InterruptedException {
    String topic1 = "new_topic1_testKafkaRDD";
    String topic2 = "new_topic2_testKafkaRDD";

    String[] topic1data = createTopicAndSendData(topic1);
    String[] topic2data = createTopicAndSendData(topic2);

    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestUtils.brokerAddress());
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put("spark.kafka.poll.time", "1000");

    OffsetRange[] offsetRanges = {
        OffsetRange.create(topic1, 0, 0, 1),
        OffsetRange.create(topic2, 0, 0, 1)
    };

    HashMap<TopicAndPartition, Broker> emptyLeaders = new HashMap<TopicAndPartition, Broker>();
    HashMap<TopicAndPartition, Broker> leaders = new HashMap<TopicAndPartition, Broker>();
    String[] hostAndPort = kafkaTestUtils.brokerAddress().split(":");
    Broker broker = Broker.create(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    leaders.put(new TopicAndPartition(topic1, 0), broker);
    leaders.put(new TopicAndPartition(topic2, 0), broker);

    JavaRDD<String> rdd1 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        kafkaParams,
        offsetRanges
    ).map(
        new Function<Tuple2<String, String>, String>() {
          @Override
          public String call(Tuple2<String, String> kv) throws Exception {
            return kv._2();
          }
        }
    );

    JavaRDD<String> rdd2 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        String.class,
        kafkaParams,
        offsetRanges,
        new Function<ConsumerRecord<String, String>, String>() {
          @Override
          public String call(ConsumerRecord<String, String> consumerRecord) throws Exception {
            return consumerRecord.value();
          }
        }
    );

    JavaRDD<String> rdd3 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        String.class,
        kafkaParams,
        offsetRanges,
        new Function<ConsumerRecord<String, String>, String>() {
          @Override
          public String call(ConsumerRecord<String, String> consumerRecord) throws Exception {
            return consumerRecord.value();
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
    kafkaTestUtils.createTopic(topic);
    kafkaTestUtils.sendMessages(topic, data);
    return data;
  }
}
