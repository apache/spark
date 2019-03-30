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

package org.apache.spark.kafka010

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol.{SASL_PLAINTEXT, SASL_SSL, SSL}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._

class KafkaTokenUtilSuite extends SparkFunSuite with KafkaDelegationTokenTest {
  private val bootStrapServers = "127.0.0.1:0"
  private val trustStoreLocation = "/path/to/trustStore"
  private val trustStorePassword = "trustStoreSecret"
  private val keyStoreLocation = "/path/to/keyStore"
  private val keyStorePassword = "keyStoreSecret"
  private val keyPassword = "keySecret"
  private val keytab = "/path/to/keytab"
  private val principal = "user@domain.com"

  private var sparkConf: SparkConf = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    sparkConf = new SparkConf()
  }

  test("checkProxyUser with proxy current user should throw exception") {
    val realUser = UserGroupInformation.createUserForTesting("realUser", Array())
    UserGroupInformation.createProxyUserForTesting("proxyUser", realUser, Array()).doAs(
      new PrivilegedExceptionAction[Unit]() {
        override def run(): Unit = {
          val thrown = intercept[IllegalArgumentException] {
            KafkaTokenUtil.checkProxyUser()
          }
          assert(thrown.getMessage contains
            "Obtaining delegation token for proxy user is not yet supported.")
        }
      }
    )
  }

  test("createAdminClientProperties without bootstrap servers should throw exception") {
    val thrown = intercept[IllegalArgumentException] {
      KafkaTokenUtil.createAdminClientProperties(sparkConf)
    }
    assert(thrown.getMessage contains
      "Tried to obtain kafka delegation token but bootstrap servers not configured.")
  }

  test("createAdminClientProperties with SASL_PLAINTEXT protocol should not include " +
      "keystore and truststore config") {
    sparkConf.set(Kafka.BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(Kafka.SECURITY_PROTOCOL, SASL_PLAINTEXT.name)
    sparkConf.set(Kafka.TRUSTSTORE_LOCATION, trustStoreLocation)
    sparkConf.set(Kafka.TRUSTSTORE_PASSWORD, trustStoreLocation)
    sparkConf.set(Kafka.KEYSTORE_LOCATION, keyStoreLocation)
    sparkConf.set(Kafka.KEYSTORE_PASSWORD, keyStorePassword)
    sparkConf.set(Kafka.KEY_PASSWORD, keyPassword)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_PLAINTEXT.name)
    assert(!adminClientProperties.containsKey("ssl.truststore.location"))
    assert(!adminClientProperties.containsKey("ssl.truststore.password"))
    assert(!adminClientProperties.containsKey("ssl.keystore.location"))
    assert(!adminClientProperties.containsKey("ssl.keystore.password"))
    assert(!adminClientProperties.containsKey("ssl.key.password"))
  }

  test("createAdminClientProperties with SASL_SSL protocol should include truststore config") {
    sparkConf.set(Kafka.BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(Kafka.SECURITY_PROTOCOL, SASL_SSL.name)
    sparkConf.set(Kafka.TRUSTSTORE_LOCATION, trustStoreLocation)
    sparkConf.set(Kafka.TRUSTSTORE_PASSWORD, trustStorePassword)
    sparkConf.set(Kafka.KEYSTORE_LOCATION, keyStoreLocation)
    sparkConf.set(Kafka.KEYSTORE_PASSWORD, keyStorePassword)
    sparkConf.set(Kafka.KEY_PASSWORD, keyPassword)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(adminClientProperties.get("ssl.truststore.location") === trustStoreLocation)
    assert(adminClientProperties.get("ssl.truststore.password") === trustStorePassword)
    assert(!adminClientProperties.containsKey("ssl.keystore.location"))
    assert(!adminClientProperties.containsKey("ssl.keystore.password"))
    assert(!adminClientProperties.containsKey("ssl.key.password"))
  }

  test("createAdminClientProperties with SSL protocol should include keystore and truststore " +
      "config") {
    sparkConf.set(Kafka.BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(Kafka.SECURITY_PROTOCOL, SSL.name)
    sparkConf.set(Kafka.TRUSTSTORE_LOCATION, trustStoreLocation)
    sparkConf.set(Kafka.TRUSTSTORE_PASSWORD, trustStorePassword)
    sparkConf.set(Kafka.KEYSTORE_LOCATION, keyStoreLocation)
    sparkConf.set(Kafka.KEYSTORE_PASSWORD, keyStorePassword)
    sparkConf.set(Kafka.KEY_PASSWORD, keyPassword)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SSL.name)
    assert(adminClientProperties.get("ssl.truststore.location") === trustStoreLocation)
    assert(adminClientProperties.get("ssl.truststore.password") === trustStorePassword)
    assert(adminClientProperties.get("ssl.keystore.location") === keyStoreLocation)
    assert(adminClientProperties.get("ssl.keystore.password") === keyStorePassword)
    assert(adminClientProperties.get("ssl.key.password") === keyPassword)
  }

  test("createAdminClientProperties with global config should not set dynamic jaas config") {
    sparkConf.set(Kafka.BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(Kafka.SECURITY_PROTOCOL, SASL_SSL.name)
    setGlobalKafkaClientConfig()

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(!adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    assert(!adminClientProperties.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
  }

  test("createAdminClientProperties with keytab should set keytab dynamic jaas config") {
    sparkConf.set(Kafka.BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(Kafka.SECURITY_PROTOCOL, SASL_SSL.name)
    sparkConf.set(KEYTAB, keytab)
    sparkConf.set(PRINCIPAL, principal)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    val saslJaasConfig = adminClientProperties.getProperty(SaslConfigs.SASL_JAAS_CONFIG)
    assert(saslJaasConfig.contains("Krb5LoginModule required"))
    assert(saslJaasConfig.contains("useKeyTab=true"))
  }

  test("createAdminClientProperties without keytab should set ticket cache dynamic jaas config") {
    sparkConf.set(Kafka.BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(Kafka.SECURITY_PROTOCOL, SASL_SSL.name)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    val saslJaasConfig = adminClientProperties.getProperty(SaslConfigs.SASL_JAAS_CONFIG)
    assert(saslJaasConfig.contains("Krb5LoginModule required"))
    assert(saslJaasConfig.contains("useTicketCache=true"))
  }

  test("isGlobalJaasConfigurationProvided without global config should return false") {
    assert(!KafkaTokenUtil.isGlobalJaasConfigurationProvided)
  }

  test("isGlobalJaasConfigurationProvided with global config should return false") {
    setGlobalKafkaClientConfig()

    assert(KafkaTokenUtil.isGlobalJaasConfigurationProvided)
  }

  test("isTokenAvailable without token should return false") {
    assert(!KafkaTokenUtil.isTokenAvailable())
  }

  test("isTokenAvailable with token should return true") {
    addTokenToUGI()

    assert(KafkaTokenUtil.isTokenAvailable())
  }

  test("getTokenJaasParams with token should return scram module") {
    addTokenToUGI()

    val jaasParams = KafkaTokenUtil.getTokenJaasParams(new SparkConf())

    assert(jaasParams.contains("ScramLoginModule required"))
    assert(jaasParams.contains("tokenauth=true"))
    assert(jaasParams.contains(tokenId))
    assert(jaasParams.contains(tokenPassword))
  }
}
