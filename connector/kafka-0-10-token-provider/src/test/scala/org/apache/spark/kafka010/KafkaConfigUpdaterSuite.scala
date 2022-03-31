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

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT

import org.apache.spark.SparkFunSuite

class KafkaConfigUpdaterSuite extends SparkFunSuite with KafkaDelegationTokenTest {
  private val testModule = "testModule"
  private val testKey = "testKey"
  private val testValue = "testValue"
  private val otherTestValue = "otherTestValue"

  test("set should always set value") {
    val params = Map.empty[String, String]

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .set(testKey, testValue)
      .build()

    assert(updatedParams.size() === 1)
    assert(updatedParams.get(testKey) === testValue)
  }

  test("setIfUnset without existing key should set value") {
    val params = Map.empty[String, String]

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setIfUnset(testKey, testValue)
      .build()

    assert(updatedParams.size() === 1)
    assert(updatedParams.get(testKey) === testValue)
  }

  test("setIfUnset with existing key should not set value") {
    val params = Map[String, String](testKey -> testValue)

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setIfUnset(testKey, otherTestValue)
      .build()

    assert(updatedParams.size() === 1)
    assert(updatedParams.get(testKey) === testValue)
  }

  test("setAuthenticationConfigIfNeeded with global security should not set values") {
    val params = Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServers
    )
    setSparkEnv(
      Map(
        s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers" -> bootStrapServers
      )
    )
    setGlobalKafkaClientConfig()

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded()
      .build()

    assert(updatedParams.asScala === params)
  }

  test("setAuthenticationConfigIfNeeded with token should set values") {
    val params = Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServers
    )
    testWithTokenSetValues(params) { updatedParams =>
      assert(updatedParams.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) ===
        KafkaTokenSparkConf.DEFAULT_SECURITY_PROTOCOL_CONFIG)
    }
  }

  test("setAuthenticationConfigIfNeeded with token should not override user-defined protocol") {
    val overrideProtocolName = SASL_PLAINTEXT.name
    val params = Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServers,
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> overrideProtocolName
    )
    testWithTokenSetValues(params) { updatedParams =>
      assert(updatedParams.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) ===
        overrideProtocolName)
    }
  }

  def testWithTokenSetValues(params: Map[String, String])
      (validate: (ju.Map[String, Object]) => Unit): Unit = {
    setSparkEnv(
      Map(
        s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers" -> bootStrapServers
      )
    )
    addTokenToUGI(tokenService1, tokenId1, tokenPassword1)

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded()
      .build()

    assert(updatedParams.size() === 4)
    assert(updatedParams.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === bootStrapServers)
    assert(updatedParams.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
    assert(updatedParams.get(SaslConfigs.SASL_MECHANISM) ===
      KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM)
    validate(updatedParams)
  }

  test("setAuthenticationConfigIfNeeded with invalid mechanism should throw exception") {
    val params = Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServers
    )
    setSparkEnv(
      Map(
        s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers" -> bootStrapServers,
        s"spark.kafka.clusters.$identifier1.sasl.token.mechanism" -> "intentionally_invalid"
      )
    )
    addTokenToUGI(tokenService1, tokenId1, tokenPassword1)

    val e = intercept[IllegalArgumentException] {
      KafkaConfigUpdater(testModule, params)
        .setAuthenticationConfigIfNeeded()
        .build()
    }

    assert(e.getMessage.contains("Delegation token works only with SCRAM mechanism."))
  }

  test("setAuthenticationConfigIfNeeded without security should not set values") {
    val params = Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServers
    )
    setSparkEnv(Map.empty)

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded()
      .build()

    assert(updatedParams.size() === 1)
    assert(updatedParams.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === bootStrapServers)
  }
}
