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

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

private[spark] case class KafkaTokenClusterConf(
    identifier: String,
    authBootstrapServers: String,
    targetServersRegex: String,
    securityProtocol: String,
    kerberosServiceName: String,
    trustStoreLocation: Option[String],
    trustStorePassword: Option[String],
    keyStoreLocation: Option[String],
    keyStorePassword: Option[String],
    keyPassword: Option[String],
    tokenMechanism: String) {
  override def toString: String = s"KafkaTokenClusterConf{" +
    s"identifier=$identifier, " +
    s"authBootstrapServers=$authBootstrapServers, " +
    s"targetServersRegex=$targetServersRegex, " +
    s"securityProtocol=$securityProtocol, " +
    s"kerberosServiceName=$kerberosServiceName, " +
    s"trustStoreLocation=$trustStoreLocation, " +
    s"trustStorePassword=${trustStorePassword.map(_ => "xxx")}, " +
    s"keyStoreLocation=$keyStoreLocation, " +
    s"keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
    s"keyPassword=${keyPassword.map(_ => "xxx")}, " +
    s"tokenMechanism=$tokenMechanism}"
}

private [kafka010] object KafkaTokenSparkConf extends Logging {
  val CLUSTERS_CONFIG_PREFIX = "spark.kafka.clusters."
  val DEFAULT_TARGET_SERVERS_REGEX = ".*"
  val DEFAULT_SASL_KERBEROS_SERVICE_NAME = "kafka"
  val DEFAULT_SASL_TOKEN_MECHANISM = "SCRAM-SHA-512"

  def getClusterConfig(sparkConf: SparkConf, identifier: String): KafkaTokenClusterConf = {
    val configPrefix = s"$CLUSTERS_CONFIG_PREFIX$identifier."
    val sparkClusterConf = sparkConf.getAllWithPrefix(configPrefix).toMap
    val result = KafkaTokenClusterConf(
      identifier,
      sparkClusterConf
        .getOrElse(s"auth.${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}",
          throw new NoSuchElementException(
            s"${configPrefix}auth.${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}")),
      sparkClusterConf.getOrElse(s"target.${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}.regex",
        KafkaTokenSparkConf.DEFAULT_TARGET_SERVERS_REGEX),
      sparkClusterConf.getOrElse(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL.name),
      sparkClusterConf.getOrElse(SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
        KafkaTokenSparkConf.DEFAULT_SASL_KERBEROS_SERVICE_NAME),
      sparkClusterConf.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
      sparkClusterConf.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
      sparkClusterConf.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
      sparkClusterConf.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
      sparkClusterConf.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
      sparkClusterConf.getOrElse("sasl.token.mechanism",
        KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM)
    )
    logDebug(s"getClusterConfig($identifier): $result")
    result
  }

  def getAllClusterConfigs(sparkConf: SparkConf): Set[KafkaTokenClusterConf] = {
    sparkConf.getAllWithPrefix(KafkaTokenSparkConf.CLUSTERS_CONFIG_PREFIX).toMap.keySet
      .flatMap { k =>
        val split = k.split('.')
        if (split.length > 0 && split(0).nonEmpty) {
          Some(split(0))
        } else {
          None
        }
      }.map(getClusterConfig(sparkConf, _))
  }
}
