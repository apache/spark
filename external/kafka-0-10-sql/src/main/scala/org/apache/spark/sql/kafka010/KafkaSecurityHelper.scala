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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.common.security.scram.ScramLoginModule

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.kafka010.KafkaTokenUtil

private[kafka010] object KafkaSecurityHelper extends Logging {
  def isTokenAvailable(): Boolean = {
    UserGroupInformation.getCurrentUser().getCredentials.getToken(
      KafkaTokenUtil.TOKEN_SERVICE) != null
  }

  def getTokenJaasParams(sparkConf: SparkConf): String = {
    val token = UserGroupInformation.getCurrentUser().getCredentials.getToken(
      KafkaTokenUtil.TOKEN_SERVICE)
    val username = new String(token.getIdentifier)
    val password = new String(token.getPassword)

    val loginModuleName = classOf[ScramLoginModule].getName
    val params =
      s"""
      |$loginModuleName required
      | tokenauth=true
      | serviceName="${sparkConf.get(Kafka.KERBEROS_SERVICE_NAME)}"
      | username="$username"
      | password="$password";
      """.stripMargin.replace("\n", "")
    logDebug(s"Scram JAAS params: ${params.replaceAll("password=\".*\"", "password=\"[hidden]\"")}")

    params
  }
}
