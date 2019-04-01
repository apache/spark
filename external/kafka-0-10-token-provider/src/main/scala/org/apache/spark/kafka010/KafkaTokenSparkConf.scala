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

import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Kafka

private[spark] case class KafkaTokenClusterConf(
  identifier: String,
  bootstrapServers: String,
  securityProtocol: String,
  kerberosServiceName: String,
  trustStoreLocation: Option[String],
  trustStorePassword: Option[String],
  keyStoreLocation: Option[String],
  keyStorePassword: Option[String],
  keyPassword: Option[String],
  tokenMechanism: String) {
  override def toString: String = s"KafkaTokenClusterConf{"
    s"identifier=$identifier, " +
    s"bootstrapServers=$bootstrapServers, "
    s"securityProtocol=$securityProtocol, " +
    s"kerberosServiceName=$kerberosServiceName, " +
    s"trustStoreLocation=$trustStoreLocation, " +
    s"trustStorePassword=${trustStorePassword.map(_ => "xxx")}, " +
    s"keyStoreLocation=$keyStoreLocation, "
    s"keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
    s"keyPassword=${keyPassword.map(_ => "xxx")}, " +
    s"tokenMechanism=$tokenMechanism}"
}

private[spark] class KafkaTokenSparkConf(sparkConf: SparkConf) extends Logging {
  def getClusterIdentifiers(): Array[String] = {
    val result = sparkConf.get(Kafka.CLUSTERS) match {
      case Some(clusters) => clusters.split(',').distinct
      case None => Array.empty[String]
    }
    logDebug(s"getClusterIdentifiers: ${result.mkString(",")}")
    result
  }

  def getClusterConfig(identifier: String): KafkaTokenClusterConf = {
    val result = KafkaTokenClusterConf(
      identifier,
      sparkConf.get(s"spark.kafka.$identifier.bootstrap.servers"),
      sparkConf.get(s"spark.kafka.$identifier.security.protocol", SASL_SSL.name),
      sparkConf.get(s"spark.kafka.$identifier.sasl.kerberos.service.name",
        KafkaTokenSparkConf.DEFAULT_SASL_KERBEROS_SERVICE_NAME),
      sparkConf.getOption(s"spark.kafka.$identifier.ssl.truststore.location"),
      sparkConf.getOption(s"spark.kafka.$identifier.ssl.truststore.password"),
      sparkConf.getOption(s"spark.kafka.$identifier.ssl.keystore.location"),
      sparkConf.getOption(s"spark.kafka.$identifier.ssl.keystore.password"),
      sparkConf.getOption(s"spark.kafka.$identifier.ssl.key.password"),
      sparkConf.get(s"spark.kafka.$identifier.sasl.token.mechanism",
        KafkaTokenSparkConf.DEFAULT_SASL_TOKEN_MECHANISM)
    )
    logDebug(s"getClusterConfig($identifier): $result")
    result
  }

  def getAllClusterConfigs(): Array[KafkaTokenClusterConf] = {
    return getClusterIdentifiers().map(getClusterConfig(_))
  }
}

private [kafka010] object KafkaTokenSparkConf {
  val DEFAULT_SASL_KERBEROS_SERVICE_NAME = "kafka"
  val DEFAULT_SASL_TOKEN_MECHANISM = "SCRAM-SHA-512"
}
