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

package org.apache.spark.deploy.security

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.reflect.runtime._
import scala.util.control.NonFatal


class KafkaDelegationTokenProvider
  extends HadoopDelegationTokenProvider with Logging {

  override def serviceName: String = "kafka"

  override  def obtainDelegationTokens(
     kafkaConfig: Configuration,
     sparkConf: SparkConf,
     creds: Credentials): Option[Long] = {

    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)

      val clientClass = mirror.classLoader.
        loadClass("org.apache.kafka.clients.admin.AdminClient")
      val createMethod = clientClass.getMethod("create", classOf[java.util.Properties])
      val clientInstance = createMethod.invoke(null, kafkaConfig)

      val obtainToken = mirror.classLoader.
        loadClass("org.apache.kafka.clients.admin.AdminClient").
        getMethod("createDelegationToken", clientClass.getClass)
      val tokenInstance = obtainToken.invoke(clientInstance)

      val createDelegationTokenResult = mirror.classLoader.
        loadClass("org.apache.kafka.clients.admin.CreateDelegationTokenResult")

      val getTokenMethod = createDelegationTokenResult.getMethod("delegationToken", createDelegationTokenResult.getClass)
      val token = getTokenMethod.invoke(tokenInstance)
        .asInstanceOf[Token[_ <: TokenIdentifier]]

      logInfo(s"Get token from Kafka: ${token.toString}")
      creds.addToken(token.getService, token)

    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to get token from service $serviceName", e)
    }

    None

  }

  override def delegationTokensRequired(
     sparkConf: SparkConf,
     kafkaConf: Configuration): Boolean = {

    kafkaConf.get("com.sun.security.auth.module.Krb5LoginModule") == "kerberos"

  }

}
