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

import org.apache.kafka.common.config.SaslConfigs

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.Kafka

class KafkaConfigUpdaterSuite extends SparkFunSuite with KafkaDelegationTokenTest {
  private val identifier = "cluster1"
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
    val params = Map.empty[String, String]
    setGlobalKafkaClientConfig()

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded(None)
      .build()

    assert(updatedParams.size() === 0)
  }

  test("setAuthenticationConfigIfNeeded with invalid tokenClusterId should throw exception") {
    val params = Map.empty[String, String]
    setSparkEnv(Map.empty)

    val e = intercept[NoSuchElementException] {
      KafkaConfigUpdater(testModule, params)
        .setAuthenticationConfigIfNeeded(Some("intentionally_invalid"))
        .build()
    }

    assert(e.getMessage.contains("intentionally_invalid"))
  }

  test("setAuthenticationConfigIfNeeded with token should set values") {
    val params = Map.empty[String, String]
    setSparkEnv(
      Map(
        Kafka.CLUSTERS.key -> identifier,
        s"spark.kafka.$identifier.bootstrap.servers" -> "127.0.0.1:0"
      )
    )
    addTokenToUGI(identifier)

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded(Some(identifier))
      .build()

    assert(updatedParams.size() === 2)
    assert(updatedParams.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
    assert(updatedParams.get(SaslConfigs.SASL_MECHANISM) ===
      KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM)
  }

  test("setAuthenticationConfigIfNeeded with invalid mechanism should throw exception") {
    val params = Map.empty[String, String]
    setSparkEnv(
      Map(
        Kafka.CLUSTERS.key -> identifier,
        s"spark.kafka.$identifier.bootstrap.servers" -> "127.0.0.1:0",
        s"spark.kafka.$identifier.sasl.token.mechanism" -> "intentionally_invalid"
      )
    )
    addTokenToUGI(identifier)

    val e = intercept[IllegalArgumentException] {
      KafkaConfigUpdater(testModule, params)
        .setAuthenticationConfigIfNeeded(Some(identifier))
        .build()
    }

    assert(e.getMessage.contains("Delegation token works only with SCRAM mechanism."))
  }

  test("setAuthenticationConfigIfNeeded without security should not set values") {
    val params = Map.empty[String, String]

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded(None)
      .build()

    assert(updatedParams.size() === 0)
  }
}
