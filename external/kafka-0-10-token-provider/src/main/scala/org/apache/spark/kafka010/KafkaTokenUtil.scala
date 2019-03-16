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
import java.text.SimpleDateFormat

import scala.util.control.NonFatal

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, CreateDelegationTokenOptions}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol.{SASL_PLAINTEXT, SASL_SSL, SSL}
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.security.token.delegation.DelegationToken

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

private[spark] object KafkaTokenUtil extends Logging {
  val TOKEN_KIND = new Text("KAFKA_DELEGATION_TOKEN")
  val TOKEN_SERVICE = new Text("kafka.server.delegation.token")

  private[spark] class KafkaDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
    override def getKind: Text = TOKEN_KIND
  }

  private[kafka010] def obtainToken(sparkConf: SparkConf): (Token[_ <: TokenIdentifier], Long) = {
    checkProxyUser()

    val adminClient = AdminClient.create(createAdminClientProperties(sparkConf))
    val createDelegationTokenOptions = new CreateDelegationTokenOptions()
    val createResult = adminClient.createDelegationToken(createDelegationTokenOptions)
    val token = createResult.delegationToken().get()
    printToken(token)

    (new Token[KafkaDelegationTokenIdentifier](
      token.tokenInfo.tokenId.getBytes,
      token.hmacAsBase64String.getBytes,
      TOKEN_KIND,
      TOKEN_SERVICE
    ), token.tokenInfo.expiryTimestamp)
  }

  private[kafka010] def checkProxyUser(): Unit = {
    val currentUser = UserGroupInformation.getCurrentUser()
    // Obtaining delegation token for proxy user is planned but not yet implemented
    // See https://issues.apache.org/jira/browse/KAFKA-6945
    require(!SparkHadoopUtil.get.isProxyUser(currentUser), "Obtaining delegation token for proxy " +
      "user is not yet supported.")
  }

  private[kafka010] def createAdminClientProperties(sparkConf: SparkConf): ju.Properties = {
    val adminClientProperties = new ju.Properties

    val bootstrapServers = sparkConf.get(Kafka.BOOTSTRAP_SERVERS)
    require(bootstrapServers.nonEmpty, s"Tried to obtain kafka delegation token but bootstrap " +
      "servers not configured.")
    adminClientProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.get)

    val protocol = sparkConf.get(Kafka.SECURITY_PROTOCOL)
    adminClientProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol)
    protocol match {
      case SASL_SSL.name =>
        setTrustStoreProperties(sparkConf, adminClientProperties)

      case SSL.name =>
        setTrustStoreProperties(sparkConf, adminClientProperties)
        setKeyStoreProperties(sparkConf, adminClientProperties)
        logWarning("Obtaining kafka delegation token with SSL protocol. Please " +
          "configure 2-way authentication on the broker side.")

      case SASL_PLAINTEXT.name =>
        logWarning("Obtaining kafka delegation token through plain communication channel. Please " +
          "consider the security impact.")
    }

    // There are multiple possibilities to log in and applied in the following order:
    // - JVM global security provided -> try to log in with JVM global security configuration
    //   which can be configured for example with 'java.security.auth.login.config'.
    //   For this no additional parameter needed.
    // - Keytab is provided -> try to log in with kerberos module and keytab using kafka's dynamic
    //   JAAS configuration.
    // - Keytab not provided -> try to log in with kerberos module and ticket cache using kafka's
    //   dynamic JAAS configuration.
    // Kafka client is unable to use subject from JVM which already logged in
    // to kdc (see KAFKA-7677)
    if (isGlobalJaasConfigurationProvided) {
      logDebug("JVM global security configuration detected, using it for login.")
    } else {
      adminClientProperties.put(SaslConfigs.SASL_MECHANISM, SaslConfigs.GSSAPI_MECHANISM)
      if (sparkConf.contains(KEYTAB)) {
        logDebug("Keytab detected, using it for login.")
        val jaasParams = getKeytabJaasParams(sparkConf)
        adminClientProperties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasParams)
      } else {
        logDebug("Using ticket cache for login.")
        val jaasParams = getTicketCacheJaasParams(sparkConf)
        adminClientProperties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasParams)
      }
    }

    adminClientProperties
  }

  def isGlobalJaasConfigurationProvided: Boolean = {
    try {
      JaasContext.loadClientContext(ju.Collections.emptyMap[String, Object]())
      true
    } catch {
      case NonFatal(_) => false
    }
  }

  private def setTrustStoreProperties(sparkConf: SparkConf, properties: ju.Properties): Unit = {
    sparkConf.get(Kafka.TRUSTSTORE_LOCATION).foreach { truststoreLocation =>
      properties.put("ssl.truststore.location", truststoreLocation)
    }
    sparkConf.get(Kafka.TRUSTSTORE_PASSWORD).foreach { truststorePassword =>
      properties.put("ssl.truststore.password", truststorePassword)
    }
  }

  private def setKeyStoreProperties(sparkConf: SparkConf, properties: ju.Properties): Unit = {
    sparkConf.get(Kafka.KEYSTORE_LOCATION).foreach { keystoreLocation =>
      properties.put("ssl.keystore.location", keystoreLocation)
    }
    sparkConf.get(Kafka.KEYSTORE_PASSWORD).foreach { keystorePassword =>
      properties.put("ssl.keystore.password", keystorePassword)
    }
    sparkConf.get(Kafka.KEY_PASSWORD).foreach { keyPassword =>
      properties.put("ssl.key.password", keyPassword)
    }
  }

  private def getKeytabJaasParams(sparkConf: SparkConf): String = {
    val params =
      s"""
      |${getKrb5LoginModuleName} required
      | useKeyTab=true
      | serviceName="${sparkConf.get(Kafka.KERBEROS_SERVICE_NAME)}"
      | keyTab="${sparkConf.get(KEYTAB).get}"
      | principal="${sparkConf.get(PRINCIPAL).get}";
      """.stripMargin.replace("\n", "")
    logDebug(s"Krb keytab JAAS params: $params")
    params
  }

  private def getTicketCacheJaasParams(sparkConf: SparkConf): String = {
    val serviceName = sparkConf.get(Kafka.KERBEROS_SERVICE_NAME)
    require(serviceName.nonEmpty, "Kerberos service name must be defined")

    val params =
      s"""
      |${getKrb5LoginModuleName} required
      | useTicketCache=true
      | serviceName="${sparkConf.get(Kafka.KERBEROS_SERVICE_NAME)}";
      """.stripMargin.replace("\n", "")
    logDebug(s"Krb ticket cache JAAS params: $params")
    params
  }

  /**
   * Krb5LoginModule package vary in different JVMs.
   * Please see Hadoop UserGroupInformation for further details.
   */
  private def getKrb5LoginModuleName(): String = {
    if (System.getProperty("java.vendor").contains("IBM")) {
      "com.ibm.security.auth.module.Krb5LoginModule"
    } else {
      "com.sun.security.auth.module.Krb5LoginModule"
    }
  }

  private def printToken(token: DelegationToken): Unit = {
    if (log.isDebugEnabled) {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
      logDebug("%-15s %-30s %-15s %-25s %-15s %-15s %-15s".format(
        "TOKENID", "HMAC", "OWNER", "RENEWERS", "ISSUEDATE", "EXPIRYDATE", "MAXDATE"))
      val tokenInfo = token.tokenInfo
      logDebug("%-15s [hidden] %-15s %-25s %-15s %-15s %-15s".format(
        tokenInfo.tokenId,
        tokenInfo.owner,
        tokenInfo.renewersAsString,
        dateFormat.format(tokenInfo.issueTimestamp),
        dateFormat.format(tokenInfo.expiryTimestamp),
        dateFormat.format(tokenInfo.maxTimestamp)))
    }
  }

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
