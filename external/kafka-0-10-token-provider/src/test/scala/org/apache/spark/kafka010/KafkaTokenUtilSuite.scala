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

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.security.auth.SecurityProtocol.{SASL_PLAINTEXT, SASL_SSL, SSL}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._

class KafkaTokenUtilSuite extends SparkFunSuite with KafkaDelegationTokenTest {
  private val identifier1 = "cluster1"
  private val identifier2 = "cluster2"
  private val tokenService1 = KafkaTokenUtil.getTokenService(identifier1)
  private val tokenService2 = KafkaTokenUtil.getTokenService(identifier2)
  private val bootStrapServers = "127.0.0.1:0"
  private val matchingTargetServersRegex = "127.0.0.*:0"
  private val nonMatchingTargetServersRegex = "127.0.1.*:0"
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

  test("createAdminClientProperties with SASL_PLAINTEXT protocol should not include " +
      "keystore and truststore config") {
    val clusterConf = createClusterConf(SASL_PLAINTEXT.name)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf, clusterConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_PLAINTEXT.name)
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG))
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG))
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_KEY_PASSWORD_CONFIG))
  }

  test("createAdminClientProperties with SASL_SSL protocol should include truststore config") {
    val clusterConf = createClusterConf(SASL_SSL.name)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf, clusterConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(adminClientProperties.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
      === trustStoreLocation)
    assert(adminClientProperties.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
      === trustStorePassword)
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG))
    assert(!adminClientProperties.containsKey(SslConfigs.SSL_KEY_PASSWORD_CONFIG))
  }

  test("createAdminClientProperties with SSL protocol should include keystore and truststore " +
      "config") {
    val clusterConf = createClusterConf(SSL.name)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf, clusterConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SSL.name)
    assert(adminClientProperties.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
      === trustStoreLocation)
    assert(adminClientProperties.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
      === trustStorePassword)
    assert(adminClientProperties.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) === keyStoreLocation)
    assert(adminClientProperties.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) === keyStorePassword)
    assert(adminClientProperties.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG) === keyPassword)
  }

  test("createAdminClientProperties with global config should not set dynamic jaas config") {
    val clusterConf = createClusterConf(SASL_SSL.name)
    setGlobalKafkaClientConfig()

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf, clusterConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(!adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    assert(!adminClientProperties.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
  }

  test("createAdminClientProperties with keytab should set keytab dynamic jaas config") {
    sparkConf.set(KEYTAB, keytab)
    sparkConf.set(PRINCIPAL, principal)
    val clusterConf = createClusterConf(SASL_SSL.name)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf, clusterConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    val saslJaasConfig = adminClientProperties.getProperty(SaslConfigs.SASL_JAAS_CONFIG)
    assert(saslJaasConfig.contains("Krb5LoginModule required"))
    assert(saslJaasConfig.contains(s"debug="))
    assert(saslJaasConfig.contains("useKeyTab=true"))
    assert(saslJaasConfig.contains(s"""keyTab="$keytab""""))
    assert(saslJaasConfig.contains(s"""principal="$principal""""))
  }

  test("createAdminClientProperties without keytab should set ticket cache dynamic jaas config") {
    val clusterConf = createClusterConf(SASL_SSL.name)

    val adminClientProperties = KafkaTokenUtil.createAdminClientProperties(sparkConf, clusterConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      === bootStrapServers)
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      === SASL_SSL.name)
    assert(adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    val saslJaasConfig = adminClientProperties.getProperty(SaslConfigs.SASL_JAAS_CONFIG)
    assert(saslJaasConfig.contains("Krb5LoginModule required"))
    assert(saslJaasConfig.contains(s"debug="))
    assert(saslJaasConfig.contains("useTicketCache=true"))
  }

  test("isGlobalJaasConfigurationProvided without global config should return false") {
    assert(!KafkaTokenUtil.isGlobalJaasConfigurationProvided)
  }

  test("isGlobalJaasConfigurationProvided with global config should return false") {
    setGlobalKafkaClientConfig()

    assert(KafkaTokenUtil.isGlobalJaasConfigurationProvided)
  }

  test("findMatchingToken without token should return None") {
    assert(KafkaTokenUtil.findMatchingToken(sparkConf, bootStrapServers) === None)
  }

  test("findMatchingToken with non-matching tokens should return None") {
    sparkConf.set(s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers", bootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.target.bootstrap.servers.regex",
      nonMatchingTargetServersRegex)
    sparkConf.set(s"spark.kafka.clusters.$identifier2.bootstrap.servers", bootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier2.target.bootstrap.servers.regex",
      matchingTargetServersRegex)
    addTokenToUGI(tokenService1)
    addTokenToUGI(new Text("intentionally_garbage"))

    assert(KafkaTokenUtil.findMatchingToken(sparkConf, bootStrapServers) === None)
  }

  test("findMatchingToken with one matching token should return cluster configuration") {
    sparkConf.set(s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers", bootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.target.bootstrap.servers.regex",
      matchingTargetServersRegex)
    addTokenToUGI(tokenService1)

    assert(KafkaTokenUtil.findMatchingToken(sparkConf, bootStrapServers) ===
      Some(KafkaTokenSparkConf.getClusterConfig(sparkConf, identifier1)))
  }

  test("findMatchingToken with multiple matching tokens should throw exception") {
    sparkConf.set(s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers", bootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.target.bootstrap.servers.regex",
      matchingTargetServersRegex)
    sparkConf.set(s"spark.kafka.clusters.$identifier2.auth.bootstrap.servers", bootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier2.target.bootstrap.servers.regex",
      matchingTargetServersRegex)
    addTokenToUGI(tokenService1)
    addTokenToUGI(tokenService2)

    val thrown = intercept[IllegalArgumentException] {
      KafkaTokenUtil.findMatchingToken(sparkConf, bootStrapServers)
    }
    assert(thrown.getMessage.contains("More than one delegation token matches"))
  }

  test("getTokenJaasParams with token should return scram module") {
    addTokenToUGI(tokenService1)
    val clusterConf = createClusterConf(SASL_SSL.name)

    val jaasParams = KafkaTokenUtil.getTokenJaasParams(clusterConf)

    assert(jaasParams.contains("ScramLoginModule required"))
    assert(jaasParams.contains("tokenauth=true"))
    assert(jaasParams.contains(tokenId))
    assert(jaasParams.contains(tokenPassword))
  }

  private def createClusterConf(securityProtocol: String): KafkaTokenClusterConf = {
    KafkaTokenClusterConf(
      identifier1,
      bootStrapServers,
      KafkaTokenSparkConf.DEFAULT_TARGET_SERVERS_REGEX,
      securityProtocol,
      KafkaTokenSparkConf.DEFAULT_SASL_KERBEROS_SERVICE_NAME,
      Some(trustStoreLocation),
      Some(trustStorePassword),
      Some(keyStoreLocation),
      Some(keyStorePassword),
      Some(keyPassword),
      KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM)
  }
}
