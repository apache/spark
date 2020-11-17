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
package org.apache.spark.examples.sql.streaming

import java.util.UUID

import org.apache.kafka.common.security.auth.SecurityProtocol

import org.apache.spark.sql.SparkSession

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: StructuredKerberizedKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
 *     [<checkpoint-location>]
 *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
 *   comma-separated list of host:port.
 *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
 *   'subscribePattern'.
 *   |- <assign> Specific TopicPartitions to consume. Json string
 *   |  {"topicA":[0,1],"topicB":[2,4]}.
 *   |- <subscribe> The topic list to subscribe. A comma-separated list of
 *   |  topics.
 *   |- <subscribePattern> The pattern used to subscribe to topic(s).
 *   |  Java regex string.
 *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
 *   |  specified for Kafka source.
 *   <topics> Different value format depends on the value of 'subscribe-type'.
 *   <checkpoint-location> Directory in which to create checkpoints. If not
 *   provided, defaults to a randomized directory in /tmp.
 *
 * Example:
 *   Yarn client:
 *    $ bin/run-example --files ${jaas_path}/kafka_jaas.conf,${keytab_path}/kafka.service.keytab \
 *      --driver-java-options "-Djava.security.auth.login.config=${path}/kafka_driver_jaas.conf" \
 *      --conf \
 *      "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_jaas.conf" \
 *      --master yarn
 *      sql.streaming.StructuredKerberizedKafkaWordCount broker1-host:port,broker2-host:port \
 *      subscribe topic1,topic2
 *   Yarn cluster:
 *    $ bin/run-example --files \
 *      ${jaas_path}/kafka_jaas.conf,${keytab_path}/kafka.service.keytab,${krb5_path}/krb5.conf \
 *      --driver-java-options \
 *      "-Djava.security.auth.login.config=./kafka_jaas.conf \
 *      -Djava.security.krb5.conf=./krb5.conf" \
 *      --conf \
 *      "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_jaas.conf" \
 *      --master yarn --deploy-mode cluster \
 *      sql.streaming.StructuredKerberizedKafkaWordCount broker1-host:port,broker2-host:port \
 *      subscribe topic1,topic2
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

object StructuredKerberizedKafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredKerberizedKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics, _*) = args
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder()
      .appName("StructuredKerberizedKafkaWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .option("kafka.security.protocol", SecurityProtocol.SASL_PLAINTEXT.name)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
