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

import org.apache.kafka.common.security.auth.SecurityProtocol.{SASL_SSL, SSL}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}

class KafkaTokenSparkConfSuite extends SparkFunSuite with BeforeAndAfterEach {
  private val identifier1 = "cluster1"
  private val identifier2 = "cluster2"
  private val authBootStrapServers = "127.0.0.1:0"
  private val targetServersRegex = "127.0.0.*:0"
  private val securityProtocol = SSL.name
  private val kerberosServiceName = "kafka1"
  private val trustStoreLocation = "/path/to/trustStore"
  private val trustStorePassword = "trustStoreSecret"
  private val keyStoreLocation = "/path/to/keyStore"
  private val keyStorePassword = "keyStoreSecret"
  private val keyPassword = "keySecret"
  private val tokenMechanism = "SCRAM-SHA-256"

  private var sparkConf: SparkConf = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    sparkConf = new SparkConf()
  }

  test("getClusterConfig should trow exception when not exists") {
    val thrown = intercept[NoSuchElementException] {
      KafkaTokenSparkConf.getClusterConfig(sparkConf, "invalid")
    }
    assert(thrown.getMessage contains "spark.kafka.clusters.invalid.auth.bootstrap.servers")
  }

  test("getClusterConfig should return entry with defaults") {
    sparkConf.set(s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers", authBootStrapServers)

    val clusterConfig = KafkaTokenSparkConf.getClusterConfig(sparkConf, identifier1)
    assert(clusterConfig.identifier === identifier1)
    assert(clusterConfig.authBootstrapServers === authBootStrapServers)
    assert(clusterConfig.targetServersRegex === KafkaTokenSparkConf.DEFAULT_TARGET_SERVERS_REGEX)
    assert(clusterConfig.securityProtocol === SASL_SSL.name)
    assert(clusterConfig.kerberosServiceName ===
      KafkaTokenSparkConf.DEFAULT_SASL_KERBEROS_SERVICE_NAME)
    assert(clusterConfig.trustStoreLocation === None)
    assert(clusterConfig.trustStorePassword === None)
    assert(clusterConfig.keyStoreLocation === None)
    assert(clusterConfig.keyStorePassword === None)
    assert(clusterConfig.keyPassword === None)
    assert(clusterConfig.tokenMechanism === KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM)
  }

  test("getClusterConfig should return entry overwrite defaults") {
    sparkConf.set(s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers", authBootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.target.bootstrap.servers.regex",
      targetServersRegex)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.security.protocol", securityProtocol)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.sasl.kerberos.service.name",
      kerberosServiceName)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.ssl.truststore.location", trustStoreLocation)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.ssl.truststore.password", trustStorePassword)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.ssl.keystore.location", keyStoreLocation)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.ssl.keystore.password", keyStorePassword)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.ssl.key.password", keyPassword)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.sasl.token.mechanism", tokenMechanism)

    val clusterConfig = KafkaTokenSparkConf.getClusterConfig(sparkConf, identifier1)
    assert(clusterConfig.identifier === identifier1)
    assert(clusterConfig.authBootstrapServers === authBootStrapServers)
    assert(clusterConfig.targetServersRegex === targetServersRegex)
    assert(clusterConfig.securityProtocol === securityProtocol)
    assert(clusterConfig.kerberosServiceName === kerberosServiceName)
    assert(clusterConfig.trustStoreLocation === Some(trustStoreLocation))
    assert(clusterConfig.trustStorePassword === Some(trustStorePassword))
    assert(clusterConfig.keyStoreLocation === Some(keyStoreLocation))
    assert(clusterConfig.keyStorePassword === Some(keyStorePassword))
    assert(clusterConfig.keyPassword === Some(keyPassword))
    assert(clusterConfig.tokenMechanism === tokenMechanism)
  }

  test("getClusterConfig should return specified kafka params") {
    sparkConf.set(s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers", authBootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier1.kafka.customKey", "customValue")

    val clusterConfig = KafkaTokenSparkConf.getClusterConfig(sparkConf, identifier1)
    assert(clusterConfig.identifier === identifier1)
    assert(clusterConfig.authBootstrapServers === authBootStrapServers)
    assert(clusterConfig.specifiedKafkaParams === Map("customKey" -> "customValue"))
  }

  test("getAllClusterConfigs should return empty list when nothing configured") {
    assert(KafkaTokenSparkConf.getAllClusterConfigs(sparkConf).isEmpty)
  }

  test("getAllClusterConfigs should return empty list with malformed configuration") {
    sparkConf.set(s"spark.kafka.clusters.", authBootStrapServers)
    assert(KafkaTokenSparkConf.getAllClusterConfigs(sparkConf).isEmpty)
  }

  test("getAllClusterConfigs should return multiple entries") {
    sparkConf.set(s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers", authBootStrapServers)
    sparkConf.set(s"spark.kafka.clusters.$identifier2.auth.bootstrap.servers", authBootStrapServers)

    val clusterConfigs = KafkaTokenSparkConf.getAllClusterConfigs(sparkConf)
    assert(clusterConfigs.size === 2)
    clusterConfigs.foreach { clusterConfig =>
      assert(clusterConfig.authBootstrapServers === authBootStrapServers)
      assert(clusterConfig.targetServersRegex === KafkaTokenSparkConf.DEFAULT_TARGET_SERVERS_REGEX)
      assert(clusterConfig.securityProtocol === SASL_SSL.name)
      assert(clusterConfig.kerberosServiceName ===
        KafkaTokenSparkConf.DEFAULT_SASL_KERBEROS_SERVICE_NAME)
      assert(clusterConfig.trustStoreLocation === None)
      assert(clusterConfig.trustStorePassword === None)
      assert(clusterConfig.keyStoreLocation === None)
      assert(clusterConfig.keyStorePassword === None)
      assert(clusterConfig.keyPassword === None)
      assert(clusterConfig.tokenMechanism === KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM)
    }
  }
}
