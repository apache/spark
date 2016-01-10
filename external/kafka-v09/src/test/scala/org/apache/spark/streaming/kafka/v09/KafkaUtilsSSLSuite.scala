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

package org.apache.spark.streaming.kafka.v09

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class KafkaUtilsSSLSuite extends SparkFunSuite with BeforeAndAfterAll {
  private var sc: SparkContext = _

  private val pathToKeyStore = "/path/to/ssl_keystore"
  private val pathToTrustStore = "/path/to/ssl_truststore"
  private val keystorePasswd = "keystore_secret_pass"
  private val truststorePasswd = "truststore_secret_pass"
  private val keyPasswd = "key_secret_pass"

  private val sparkSslProperties = Map[String, String] (
    "spark.ssl.kafka.enabled" -> "true",
    "spark.ssl.kafka.keyStore" -> pathToKeyStore,
    "spark.ssl.kafka.keyStorePassword" -> keystorePasswd,
    "spark.ssl.kafka.trustStore" -> pathToTrustStore,
    "spark.ssl.kafka.trustStorePassword" -> truststorePasswd,
    "spark.ssl.kafka.keyPassword" -> keyPasswd
  )

  private val kafkaSslProperties = Map[String, String] (
    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SSL",
    SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> pathToKeyStore,
    SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> keystorePasswd,
    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> pathToTrustStore,
    SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> truststorePasswd,
    SslConfigs.SSL_KEY_PASSWORD_CONFIG -> keyPasswd
  )

  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)

  override def beforeAll {
    sparkConf.setAll(sparkSslProperties)
    sc = new SparkContext(sparkConf)
  }

  override def afterAll {
    if (sc != null) {
      sc.stop
      sc = null
    }
  }

  test("Check adding SSL properties to Kafka parameters") {
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9093",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> "test-consumer",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "spark.kafka.poll.time" -> "100")

    val kafkaParamsWithSSL = KafkaUtils.addSSLOptions(kafkaParams, sc)

    kafkaSslProperties.foreach {
      case (k, v) => assert(kafkaParamsWithSSL.get(k).get.toString == v)
    }
  }

}
