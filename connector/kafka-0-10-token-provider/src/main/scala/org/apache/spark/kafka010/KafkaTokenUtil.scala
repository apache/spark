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
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, CreateDelegationTokenOptions}
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol.{SASL_PLAINTEXT, SASL_SSL, SSL}
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.security.token.delegation.DelegationToken

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.{SecurityUtils, Utils}
import org.apache.spark.util.Utils.REDACTION_REPLACEMENT_TEXT

object KafkaTokenUtil extends Logging {
  val TOKEN_KIND = new Text("KAFKA_DELEGATION_TOKEN")
  private val TOKEN_SERVICE_PREFIX = "kafka.server.delegation.token"
  private val DATE_TIME_FORMATTER =
    DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm")
      .withZone(ZoneId.systemDefault())

  def getTokenService(identifier: String): Text =
    new Text(s"$TOKEN_SERVICE_PREFIX.$identifier")

  private def getClusterIdentifier(service: Text): String =
    service.toString().replace(s"$TOKEN_SERVICE_PREFIX.", "")

  class KafkaDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
    override def getKind: Text = TOKEN_KIND
  }

  def obtainToken(
      sparkConf: SparkConf,
      clusterConf: KafkaTokenClusterConf): (Token[KafkaDelegationTokenIdentifier], Long) = {
    checkProxyUser()

    val adminClient = AdminClient.create(createAdminClientProperties(sparkConf, clusterConf))
    val createDelegationTokenOptions = new CreateDelegationTokenOptions()
    val createResult = adminClient.createDelegationToken(createDelegationTokenOptions)
    val token = createResult.delegationToken().get()
    printToken(token)

    (new Token[KafkaDelegationTokenIdentifier](
      token.tokenInfo.tokenId.getBytes,
      token.hmacAsBase64String.getBytes,
      TOKEN_KIND,
      getTokenService(clusterConf.identifier)
    ), token.tokenInfo.expiryTimestamp)
  }

  def checkProxyUser(): Unit = {
    val currentUser = UserGroupInformation.getCurrentUser()
    // Obtaining delegation token for proxy user is planned but not yet implemented
    // See https://issues.apache.org/jira/browse/KAFKA-6945
    require(!SparkHadoopUtil.get.isProxyUser(currentUser), "Obtaining delegation token for proxy " +
      "user is not yet supported.")
  }

  def createAdminClientProperties(
      sparkConf: SparkConf,
      clusterConf: KafkaTokenClusterConf): ju.Properties = {
    val adminClientProperties = new ju.Properties

    adminClientProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
      clusterConf.authBootstrapServers)

    adminClientProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
      clusterConf.securityProtocol)
    clusterConf.securityProtocol match {
      case SASL_SSL.name =>
        setTrustStoreProperties(clusterConf, adminClientProperties)

      case SSL.name =>
        setTrustStoreProperties(clusterConf, adminClientProperties)
        setKeyStoreProperties(clusterConf, adminClientProperties)
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
        val keyTab = sparkConf.get(KEYTAB).get
        val principal = sparkConf.get(PRINCIPAL).get
        val jaasParams = getKeytabJaasParams(keyTab, principal, clusterConf.kerberosServiceName)
        adminClientProperties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasParams)
      } else {
        logDebug("Using ticket cache for login.")
        val jaasParams = getTicketCacheJaasParams(clusterConf)
        adminClientProperties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasParams)
      }
    }

    logDebug("AdminClient params before specified params: " +
      s"${KafkaRedactionUtil.redactParams(adminClientProperties.asScala.toSeq)}")

    clusterConf.specifiedKafkaParams.foreach { param =>
      adminClientProperties.setProperty(param._1, param._2)
    }

    logDebug("AdminClient params after specified params: " +
      s"${KafkaRedactionUtil.redactParams(adminClientProperties.asScala.toSeq)}")

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

  private def setTrustStoreProperties(
      clusterConf: KafkaTokenClusterConf,
      properties: ju.Properties): Unit = {
    clusterConf.trustStoreType.foreach { truststoreType =>
      properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoreType)
    }
    clusterConf.trustStoreLocation.foreach { truststoreLocation =>
      properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation)
    }
    clusterConf.trustStorePassword.foreach { truststorePassword =>
      properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword)
    }
  }

  private def setKeyStoreProperties(
      clusterConf: KafkaTokenClusterConf,
      properties: ju.Properties): Unit = {
    clusterConf.keyStoreType.foreach { keystoreType =>
      properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystoreType)
    }
    clusterConf.keyStoreLocation.foreach { keystoreLocation =>
      properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation)
    }
    clusterConf.keyStorePassword.foreach { keystorePassword =>
      properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword)
    }
    clusterConf.keyPassword.foreach { keyPassword =>
      properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword)
    }
  }

  def getKeytabJaasParams(
      keyTab: String,
      principal: String,
      kerberosServiceName: String): String = {
    val params =
      s"""
      |${SecurityUtils.getKrb5LoginModuleName()} required
      | debug=${SecurityUtils.isGlobalKrbDebugEnabled()}
      | useKeyTab=true
      | serviceName="$kerberosServiceName"
      | keyTab="$keyTab"
      | principal="$principal";
      """.stripMargin.replace("\n", "")
    logDebug(s"Krb keytab JAAS params: $params")
    params
  }

  private def getTicketCacheJaasParams(clusterConf: KafkaTokenClusterConf): String = {
    val params =
      s"""
      |${SecurityUtils.getKrb5LoginModuleName()} required
      | debug=${SecurityUtils.isGlobalKrbDebugEnabled()}
      | useTicketCache=true
      | serviceName="${clusterConf.kerberosServiceName}";
      """.stripMargin.replace("\n", "").trim
    logDebug(s"Krb ticket cache JAAS params: $params")
    params
  }

  private def printToken(token: DelegationToken): Unit = {
    if (log.isDebugEnabled) {
      logDebug("%-15s %-30s %-15s %-25s %-15s %-15s %-15s".format(
        "TOKENID", "HMAC", "OWNER", "RENEWERS", "ISSUEDATE", "EXPIRYDATE", "MAXDATE"))
      val tokenInfo = token.tokenInfo
      logDebug("%-15s %-15s %-15s %-25s %-15s %-15s %-15s".format(
        tokenInfo.tokenId,
        REDACTION_REPLACEMENT_TEXT,
        tokenInfo.owner,
        tokenInfo.renewersAsString,
        DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(tokenInfo.issueTimestamp)),
        DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(tokenInfo.expiryTimestamp)),
        DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(tokenInfo.maxTimestamp))))
    }
  }

  def findMatchingTokenClusterConfig(
      sparkConf: SparkConf,
      bootStrapServers: String): Option[KafkaTokenClusterConf] = {
    val tokens = UserGroupInformation.getCurrentUser().getCredentials.getAllTokens.asScala
    val clusterConfigs = tokens
      .filter(_.getService().toString().startsWith(TOKEN_SERVICE_PREFIX))
      .map { token =>
        KafkaTokenSparkConf.getClusterConfig(sparkConf, getClusterIdentifier(token.getService()))
      }
      .filter { clusterConfig =>
        val pattern = Pattern.compile(clusterConfig.targetServersRegex)
        Utils.stringToSeq(bootStrapServers).exists(pattern.matcher(_).matches())
      }
    require(clusterConfigs.size <= 1, "More than one delegation token matches the following " +
      s"bootstrap servers: $bootStrapServers.")
    clusterConfigs.headOption
  }

  def getTokenJaasParams(clusterConf: KafkaTokenClusterConf): String = {
    val token = UserGroupInformation.getCurrentUser().getCredentials.getToken(
      getTokenService(clusterConf.identifier))
    require(token != null, s"Token for identifier ${clusterConf.identifier} must exist")
    val username = new String(token.getIdentifier)
    val password = new String(token.getPassword)

    val loginModuleName = classOf[ScramLoginModule].getName
    val params =
      s"""
      |$loginModuleName required
      | tokenauth=true
      | serviceName="${clusterConf.kerberosServiceName}"
      | username="$username"
      | password="$password";
      """.stripMargin.replace("\n", "").trim
    logDebug(s"Scram JAAS params: ${KafkaRedactionUtil.redactJaasParam(params)}")

    params
  }

  def needTokenUpdate(
      params: ju.Map[String, Object],
      clusterConfig: Option[KafkaTokenClusterConf]): Boolean = {
    if (clusterConfig.isDefined && params.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
      logDebug("Delegation token used by connector, checking if uses the latest token.")
      val connectorJaasParams = params.get(SaslConfigs.SASL_JAAS_CONFIG).asInstanceOf[String]
      getTokenJaasParams(clusterConfig.get) != connectorJaasParams
    } else {
      false
    }
  }
}
