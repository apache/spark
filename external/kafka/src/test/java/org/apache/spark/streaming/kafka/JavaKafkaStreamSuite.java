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
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import junit.framework.Assert;

import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;

public class JavaKafkaStreamSuite implements Serializable {
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
    ssc = new JavaStreamingContext(sparkConf, new Duration(500));
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
    String topic = "topic1";
    HashMap<String, Integer> topics = new HashMap<String, Integer>();
    topics.put(topic, 1);

    HashMap<String, Integer> sent = new HashMap<String, Integer>();
    sent.put("a", 5);
    sent.put("b", 3);
    sent.put("c", 10);

    suiteBase.createTopic(topic);
    HashMap<String, Object> tmp = new HashMap<String, Object>(sent);
    suiteBase.sendMessages(topic,
        JavaConverters.mapAsScalaMapConverter(tmp).asScala().toMap(
            Predef.<Tuple2<String, Object>>conforms())
    );

    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("zookeeper.connect", suiteBase.zkAddress());
    kafkaParams.put("group.id", "test-consumer-" + random.nextInt(10000));
    kafkaParams.put("auto.offset.reset", "smallest");

    JavaPairDStream<String, String> stream = KafkaUtils.createStream(ssc,
      String.class,
      String.class,
      StringDecoder.class,
      StringDecoder.class,
      kafkaParams,
      topics,
      StorageLevel.MEMORY_ONLY_SER());

    final HashMap<String, Long> result = new HashMap<String, Long>();

    JavaDStream<String> words = stream.map(
      new Function<Tuple2<String, String>, String>() {
        @Override
        public String call(Tuple2<String, String> tuple2) throws Exception {
          return tuple2._2();
        }
      }
    );

    words.countByValue().foreachRDD(
      new Function<JavaPairRDD<String, Long>, Void>() {
        @Override
        public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
          List<Tuple2<String, Long>> ret = rdd.collect();
          for (Tuple2<String, Long> r : ret) {
            if (result.containsKey(r._1())) {
              result.put(r._1(), result.get(r._1()) + r._2());
            } else {
              result.put(r._1(), r._2());
            }
          }

          return null;
        }
      }
    );

    ssc.start();
    long startTime = System.currentTimeMillis();
    boolean sizeMatches = false;
    while (!sizeMatches && System.currentTimeMillis() - startTime < 20000) {
      sizeMatches = sent.size() == result.size();
      Thread.sleep(200);
    }
    Assert.assertEquals(sent.size(), result.size());
    for (String k : sent.keySet()) {
      Assert.assertEquals(sent.get(k).intValue(), result.get(k).intValue());
    }
    ssc.stop();
  }
}
