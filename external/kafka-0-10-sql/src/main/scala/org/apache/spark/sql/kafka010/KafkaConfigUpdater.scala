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

import scala.collection.JavaConverters._

import org.apache.kafka.common.config.SaslConfigs

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Kafka
import org.apache.spark.kafka010.KafkaTokenUtil

/**
 * Class to conveniently update Kafka config params, while logging the changes
 */
private[kafka010] case class KafkaConfigUpdater(module: String, kafkaParams: Map[String, Object])
    extends Logging {
  private val map = new ju.HashMap[String, Object](kafkaParams.asJava)

  def set(key: String, value: Object): this.type = {
    map.put(key, value)
    logDebug(s"$module: Set $key to $value, earlier value: ${kafkaParams.getOrElse(key, "")}")
    this
  }

  def setIfUnset(key: String, value: Object): this.type = {
    if (!map.containsKey(key)) {
      map.put(key, value)
      logDebug(s"$module: Set $key to $value")
    }
    this
  }

  def setAuthenticationConfigIfNeeded(): this.type = {
    // There are multiple possibilities to log in and applied in the following order:
    // - JVM global security provided -> try to log in with JVM global security configuration
    //   which can be configured for example with 'java.security.auth.login.config'.
    //   For this no additional parameter needed.
    // - Token is provided -> try to log in with scram module using kafka's dynamic JAAS
    //   configuration.
    if (KafkaTokenUtil.isGlobalJaasConfigurationProvided) {
      logDebug("JVM global security configuration detected, using it for login.")
    } else if (KafkaSecurityHelper.isTokenAvailable()) {
      logDebug("Delegation token detected, using it for login.")
      val jaasParams = KafkaSecurityHelper.getTokenJaasParams(SparkEnv.get.conf)
      set(SaslConfigs.SASL_JAAS_CONFIG, jaasParams)
      val mechanism = SparkEnv.get.conf.get(Kafka.TOKEN_SASL_MECHANISM)
      require(mechanism.startsWith("SCRAM"),
        "Delegation token works only with SCRAM mechanism.")
      set(SaslConfigs.SASL_MECHANISM, mechanism)
    }
    this
  }

  def build(): ju.Map[String, Object] = map
}
