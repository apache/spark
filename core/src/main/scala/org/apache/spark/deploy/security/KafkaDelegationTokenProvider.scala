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

import scala.reflect.runtime.universe
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{KAFKA_DELEGATION_TOKEN_ENABLED, KAFKA_SECURITY_PROTOCOL}
import org.apache.spark.util.Utils

private[security] class KafkaDelegationTokenProvider
  extends HadoopDelegationTokenProvider with Logging {

  override def serviceName: String = "kafka"

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      val obtainToken = mirror.classLoader.
        loadClass("org.apache.spark.sql.kafka010.TokenUtil").
        getMethod("obtainToken", classOf[SparkConf])

      logDebug("Attempting to fetch Kafka security token.")
      val token = obtainToken.invoke(null, sparkConf)
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
      hadoopConf: Configuration): Boolean = {
    sparkConf.get(KAFKA_DELEGATION_TOKEN_ENABLED) &&
      sparkConf.get(KAFKA_SECURITY_PROTOCOL).startsWith("SASL")
  }
}
