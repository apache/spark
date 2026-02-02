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

// scalastyle:off println
package org.apache.spark.examples.streaming

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKerberizedKafkaWordCount <brokers> <topics>
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
 *      streaming.DirectKerberizedKafkaWordCount broker1-host:port,broker2-host:port \
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
 *      streaming.DirectKerberizedKafkaWordCount broker1-host:port,broker2-host:port \
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
object DirectKerberizedKafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: DirectKerberizedKafkaWordCount <brokers> <groupId> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <groupId> is a consumer group name to consume from topics
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, groupId, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKerberizedKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SASL_PLAINTEXT.name)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
