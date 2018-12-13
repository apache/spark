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

package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.util.UUID
import javax.security.auth.login.{AppConfigurationEntry, Configuration}

import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token
import org.apache.kafka.common.config.SaslConfigs
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.deploy.security.KafkaTokenUtil
import org.apache.spark.deploy.security.KafkaTokenUtil.KafkaDelegationTokenIdentifier
import org.apache.spark.internal.config._

class KafkaConfigUpdaterSuite extends SparkFunSuite with BeforeAndAfterEach {
  private val testModule = "testModule"
  private val testKey = "testKey"
  private val testValue = "testValue"
  private val otherTestValue = "otherTestValue"
  private val tokenId = "tokenId" + UUID.randomUUID().toString
  private val tokenPassword = "tokenPassword" + UUID.randomUUID().toString

  private class KafkaJaasConfiguration extends Configuration {
    val entry =
      new AppConfigurationEntry(
        "DummyModule",
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        ju.Collections.emptyMap[String, Object]()
      )

    override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
      if (name.equals("KafkaClient")) {
        Array(entry)
      } else {
        null
      }
    }
  }

  override def afterEach(): Unit = {
    try {
      resetGlobalConfig
      resetUGI
      resetSparkEnv
    } finally {
      super.afterEach()
    }
  }

  private def setGlobalKafkaClientConfig(): Unit = {
    Configuration.setConfiguration(new KafkaJaasConfiguration)
  }

  private def resetGlobalConfig: Unit = {
    Configuration.setConfiguration(null)
  }

  private def addTokenToUGI(): Unit = {
    val token = new Token[KafkaDelegationTokenIdentifier](
      tokenId.getBytes,
      tokenPassword.getBytes,
      KafkaTokenUtil.TOKEN_KIND,
      KafkaTokenUtil.TOKEN_SERVICE
    )
    val creds = new Credentials()
    creds.addToken(KafkaTokenUtil.TOKEN_SERVICE, token)
    UserGroupInformation.getCurrentUser.addCredentials(creds)
  }

  private def resetUGI: Unit = {
    UserGroupInformation.setLoginUser(null)
  }

  private def setSparkEnv(settings: Traversable[(String, String)]): Unit = {
    val conf = new SparkConf().setAll(settings)
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)
  }

  private def resetSparkEnv: Unit = {
    SparkEnv.set(null)
  }

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
      .setAuthenticationConfigIfNeeded()
      .build()

    assert(updatedParams.size() === 0)
  }

  test("setAuthenticationConfigIfNeeded with token should set values") {
    val params = Map.empty[String, String]
    setSparkEnv(Map.empty)
    addTokenToUGI()

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded()
      .build()

    assert(updatedParams.size() === 2)
    assert(updatedParams.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
    assert(updatedParams.get(SaslConfigs.SASL_MECHANISM) === "SCRAM-SHA-512")
  }

  test("setAuthenticationConfigIfNeeded with token and invalid mechanism should throw exception") {
    val params = Map.empty[String, String]
    setSparkEnv(Map[String, String](Kafka.TOKEN_SASL_MECHANISM.key -> "INVALID"))
    addTokenToUGI()

    val e = intercept[IllegalArgumentException] {
      KafkaConfigUpdater(testModule, params)
        .setAuthenticationConfigIfNeeded()
        .build()
    }

    assert(e.getMessage.contains("Delegation token works only with SCRAM mechanism."))
  }

  test("setAuthenticationConfigIfNeeded without security should not set values") {
    val params = Map.empty[String, String]

    val updatedParams = KafkaConfigUpdater(testModule, params)
      .setAuthenticationConfigIfNeeded()
      .build()

    assert(updatedParams.size() === 0)
  }
}
