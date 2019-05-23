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

  protected val tokenId = "tokenId" + ju.UUID.randomUUID().toString
  protected val tokenPassword = "tokenPassword" + ju.UUID.randomUUID().toString

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
      Configuration.setConfiguration(null)
      UserGroupInformation.setLoginUser(null)
      SparkEnv.set(null)
    } finally {
      super.afterEach()
    }
  }

  protected def setGlobalKafkaClientConfig(): Unit = {
    Configuration.setConfiguration(new KafkaJaasConfiguration)
  }

  protected def addTokenToUGI(tokenService: Text): Unit = {
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
}
