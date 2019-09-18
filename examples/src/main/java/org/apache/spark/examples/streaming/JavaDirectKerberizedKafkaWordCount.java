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

package org.apache.spark.examples.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKerberizedKafkaWordCount <brokers> <groupId> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *   Yarn client:
 *    $ bin/run-example --files ${jaas_path}/kafka_jaas.conf,${keytab_path}/kafka.service.keytab \
 *      --driver-java-options "-Djava.security.auth.login.config=${path}/kafka_driver_jaas.conf" \
 *      --conf \
 *      "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_jaas.conf" \
 *      --master yarn
 *      streaming.JavaDirectKerberizedKafkaWordCount broker1-host:port,broker2-host:port \
 *      consumer-group topic1,topic2
 *   Yarn cluster:
 *    $ bin/run-example --files \
 *      ${jaas_path}/kafka_jaas.conf,${keytab_path}/kafka.service.keytab,${krb5_path}/krb5.conf \
 *      --driver-java-options \
 *      "-Djava.security.auth.login.config=./kafka_jaas.conf \
 *      -Djava.security.krb5.conf=./krb5.conf" \
 *      --conf \
 *      "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_jaas.conf" \
 *      --master yarn --deploy-mode cluster \
 *      streaming.JavaDirectKerberizedKafkaWordCount broker1-host:port,broker2-host:port \
 *      consumer-group topic1,topic2
 *
 * kafka_jaas.conf can manually create, template as:
 *   KafkaClient {
 *     com.sun.security.auth.module.Krb5LoginModule required
 *     keyTab="./kafka.service.keytab"
 *     useKeyTab=true
 *     storeKey=true
 *     useTicketCache=false
 *     serviceName="kafka"
 *     principal="kafka/host@EXAMPLE.COM";
 *   };
 * kafka_driver_jaas.conf (used by yarn client) and kafka_jaas.conf are basically the same
 * except for some differences at 'keyTab'. In kafka_driver_jaas.conf, 'keyTab' should be
 * "${keytab_path}/kafka.service.keytab".
 * In addition, for IBM JVMs, please use 'com.ibm.security.auth.module.Krb5LoginModule'
 * instead of 'com.sun.security.auth.module.Krb5LoginModule'.
 *
 * Note that this example uses SASL_PLAINTEXT for simplicity; however,
 * SASL_PLAINTEXT has no SSL encryption and likely be less secure. Please consider
 * using SASL_SSL in production.
 */

public final class JavaDirectKerberizedKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println(
        "Usage: JavaDirectKerberizedKafkaWordCount <brokers> <groupId> <topics>\n" +
        "  <brokers> is a list of one or more Kafka brokers\n" +
        "  <groupId> is a consumer group name to consume from topics\n" +
        "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }

    StreamingExamples.setStreamingLogLevels();

    String brokers = args[0];
    String groupId = args[1];
    String topics = args[2];

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKerberizedKafkaWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                                  SecurityProtocol.SASL_PLAINTEXT.name);

    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
      jssc,
      LocationStrategies.PreferConsistent(),
      ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(ConsumerRecord::value);
    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
      .reduceByKey((i1, i2) -> i1 + i2);
    wordCounts.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
