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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
    String topic1 = "topic1_testKafkaRDD";
    String topic2 = "topic2_testKafkaRDD";

    createTopicAndSendData(topic1);
    createTopicAndSendData(topic2);

    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestUtils.brokerAddress());
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put("spark.kafka.poll.time", "1000");

    OffsetRange[] offsetRanges = {
      OffsetRange.create(topic1, 0, 0, 1),
      OffsetRange.create(topic2, 0, 0, 1)
    };

    JavaRDD<String> rdd1 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        kafkaParams,
        offsetRanges
    ).map(
        new Function<Tuple2<String, String>, String>() {
          @Override
          public String call(Tuple2<String, String> kv) {
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
