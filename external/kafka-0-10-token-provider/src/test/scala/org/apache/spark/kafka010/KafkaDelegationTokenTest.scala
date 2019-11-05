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
import javax.security.auth.login.{AppConfigurationEntry, Configuration}

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token
import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.kafka010.KafkaTokenUtil.KafkaDelegationTokenIdentifier

/**
 * This is a trait which provides functionalities for Kafka delegation token related test suites.
 */
trait KafkaDelegationTokenTest extends BeforeAndAfterEach {
  self: SparkFunSuite =>

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  private var savedSparkEnv: SparkEnv = _

  protected val tokenId1 = "tokenId" + ju.UUID.randomUUID().toString
  protected val tokenPassword1 = "tokenPassword" + ju.UUID.randomUUID().toString
  protected val tokenId2 = "tokenId" + ju.UUID.randomUUID().toString
  protected val tokenPassword2 = "tokenPassword" + ju.UUID.randomUUID().toString

  protected val identifier1 = "cluster1"
  protected val identifier2 = "cluster2"
  protected val tokenService1 = KafkaTokenUtil.getTokenService(identifier1)
  protected val tokenService2 = KafkaTokenUtil.getTokenService(identifier2)
  protected val bootStrapServers = "127.0.0.1:0"
  protected val matchingTargetServersRegex = "127.0.0.*:0"
  protected val nonMatchingTargetServersRegex = "127.0.intentionally_non_matching.*:0"
  protected val trustStoreLocation = "/path/to/trustStore"
  protected val trustStorePassword = "trustStoreSecret"
  protected val keyStoreLocation = "/path/to/keyStore"
  protected val keyStorePassword = "keyStoreSecret"
  protected val keyPassword = "keySecret"
  protected val keytab = "/path/to/keytab"
  protected val principal = "user@domain.com"

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

  override def beforeEach(): Unit = {
    super.beforeEach()
    savedSparkEnv = SparkEnv.get
  }

  override def afterEach(): Unit = {
    try {
      Configuration.setConfiguration(null)
      UserGroupInformation.reset()
      SparkEnv.set(savedSparkEnv)
    } finally {
      super.afterEach()
    }
  }

  protected def setGlobalKafkaClientConfig(): Unit = {
    Configuration.setConfiguration(new KafkaJaasConfiguration)
  }

  protected def addTokenToUGI(tokenService: Text, tokenId: String, tokenPassword: String): Unit = {
    val token = new Token[KafkaDelegationTokenIdentifier](
      tokenId.getBytes,
      tokenPassword.getBytes,
      KafkaTokenUtil.TOKEN_KIND,
      tokenService
    )
    val creds = new Credentials()
    creds.addToken(token.getService, token)
    UserGroupInformation.getCurrentUser.addCredentials(creds)
  }

  protected def setSparkEnv(settings: Iterable[(String, String)]): Unit = {
    val conf = new SparkConf().setAll(settings)
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)
  }

  protected def createClusterConf(
      identifier: String,
      securityProtocol: String,
      specifiedKafkaParams: Map[String, String] = Map.empty): KafkaTokenClusterConf = {
    KafkaTokenClusterConf(
      identifier,
      bootStrapServers,
      KafkaTokenSparkConf.DEFAULT_TARGET_SERVERS_REGEX,
      securityProtocol,
      KafkaTokenSparkConf.DEFAULT_SASL_KERBEROS_SERVICE_NAME,
      Some(trustStoreLocation),
      Some(trustStorePassword),
      Some(keyStoreLocation),
      Some(keyStorePassword),
      Some(keyPassword),
      KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM,
      specifiedKafkaParams)
  }
}
