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

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

/**
 * Class to conveniently update Kafka config params, while logging the changes
 */
case class KafkaConfigUpdater(module: String, kafkaParams: Map[String, Object])
    extends Logging {
  private val map = new ju.HashMap[String, Object](kafkaParams.asJava)

  def set(key: String, value: Object): this.type = {
    map.put(key, value)
    if (log.isDebugEnabled()) {
      val redactedValue = KafkaRedactionUtil.redactParams(Seq((key, value))).head._2
      val redactedOldValue = KafkaRedactionUtil
        .redactParams(Seq((key, kafkaParams.getOrElse(key, "")))).head._2
      logDebug(s"$module: Set $key to $redactedValue, earlier value: $redactedOldValue")
    }
    this
  }

  def setIfUnset(key: String, value: Object): this.type = {
    if (!map.containsKey(key)) {
      map.put(key, value)
      if (log.isDebugEnabled()) {
        val redactedValue = KafkaRedactionUtil.redactParams(Seq((key, value))).head._2
        logDebug(s"$module: Set $key to $redactedValue")
      }
    }
    this
  }

  def setAuthenticationConfigIfNeeded(): this.type = {
    val bootstrapServers = kafkaParams
      .get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      .map(_.asInstanceOf[String])
      .getOrElse(throw KafkaTokenProviderExceptions.missingKafkaOption(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))

    val clusterConfig = KafkaTokenUtil.findMatchingTokenClusterConfig(SparkEnv.get.conf,
      bootstrapServers)
    setAuthenticationConfigIfNeeded(clusterConfig)
  }

  def setAuthenticationConfigIfNeeded(clusterConfig: Option[KafkaTokenClusterConf]): this.type = {
    // There are multiple possibilities to log in and applied in the following order:
    // - JVM global security provided -> try to log in with JVM global security configuration
    //   which can be configured for example with 'java.security.auth.login.config'.
    //   For this no additional parameter needed.
    // - Token is provided -> try to log in with scram module using kafka's dynamic JAAS
    //   configuration.
    if (KafkaTokenUtil.isGlobalJaasConfigurationProvided) {
      logDebug("JVM global security configuration detected, using it for login.")
    } else {
      clusterConfig.foreach { clusterConf =>
        logDebug("Delegation token detected, using it for login.")
        setIfUnset(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, clusterConf.securityProtocol)
        val jaasParams = KafkaTokenUtil.getTokenJaasParams(clusterConf)
        set(SaslConfigs.SASL_JAAS_CONFIG, jaasParams)
        require(clusterConf.tokenMechanism.startsWith("SCRAM"),
          "Delegation token works only with SCRAM mechanism.")
        set(SaslConfigs.SASL_MECHANISM, clusterConf.tokenMechanism)
      }
    }
    this
  }

  def build(): ju.Map[String, Object] = map
}
