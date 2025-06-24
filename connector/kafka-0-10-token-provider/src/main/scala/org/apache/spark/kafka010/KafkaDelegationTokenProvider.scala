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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.kafka.common.security.auth.SecurityProtocol.{SASL_PLAINTEXT, SASL_SSL, SSL}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CLUSTER_ID, SERVICE_NAME}
import org.apache.spark.security.HadoopDelegationTokenProvider

class KafkaDelegationTokenProvider
  extends HadoopDelegationTokenProvider with Logging {

  override def serviceName: String = "kafka"

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    var lowestNextRenewalDate: Option[Long] = None
    try {
      KafkaTokenSparkConf.getAllClusterConfigs(sparkConf).foreach { clusterConf =>
        try {
          if (delegationTokensRequired(clusterConf)) {
            logDebug(
              s"Attempting to fetch Kafka security token for cluster ${clusterConf.identifier}.")
            val (token, nextRenewalDate) = KafkaTokenUtil.obtainToken(sparkConf, clusterConf)
            creds.addToken(token.getService, token)
            if (lowestNextRenewalDate.isEmpty || nextRenewalDate < lowestNextRenewalDate.get) {
              lowestNextRenewalDate = Some(nextRenewalDate)
            }
          } else {
            logDebug(
              s"Cluster ${clusterConf.identifier} does not require delegation token, skipping.")
          }
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to get token from service: ${MDC(SERVICE_NAME, serviceName)} " +
              log"on cluster: ${MDC(CLUSTER_ID, clusterConf.identifier)}. If " +
              log"${MDC(SERVICE_NAME, serviceName)} is not used, please set " +
              log"spark.security.credentials.${MDC(SERVICE_NAME, serviceName)}.enabled to false", e)
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get token cluster configuration", e)
    }
    lowestNextRenewalDate
  }

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    try {
      KafkaTokenSparkConf.getAllClusterConfigs(sparkConf).exists(delegationTokensRequired(_))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get token cluster configuration", e)
        false
    }
  }

  private def delegationTokensRequired(clusterConf: KafkaTokenClusterConf): Boolean =
    clusterConf.securityProtocol == SASL_SSL.name ||
      clusterConf.securityProtocol == SSL.name ||
      clusterConf.securityProtocol == SASL_PLAINTEXT.name
}
