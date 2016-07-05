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

package org.apache.spark.deploy.yarn.token

import java.io.{ByteArrayInputStream, DataInputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

private[yarn] final class ConfigurableTokenManager private (sparkConf: SparkConf) extends Logging {
  private val tokenProviderEnabledConfig = "spark\\.yarn\\.security\\.tokens\\.(.+)\\.enabled".r
  private val tokenProviderClsConfig = "spark.yarn.security.tokens.%s.class"

  // Maintain all the registered token providers
  private val tokenProviders = mutable.HashMap[String, ServiceTokenProvider]()

  private val defaultTokenProviders = Map(
    "hdfs" -> (true, "org.apache.spark.deploy.yarn.token.HDFSTokenProvider"),
    "hive" -> (true, "org.apache.spark.deploy.yarn.token.HiveTokenProvider"),
    "hbase" -> (true, "org.apache.spark.deploy.yarn.token.HBaseTokenProvider")
  )

  // AMDelegationTokenRenewer, this will only be create and started in the AM
  private var _delegationTokenRenewer: AMDelegationTokenRenewer = null

  // ExecutorDelegationTokenUpdater, this will only be created and started in the driver and
  // executor side.
  private var _delegationTokenUpdater: ExecutorDelegationTokenUpdater = null

  def initialize(): Unit = {
    sparkConf.getAll.filter { case (key, value) =>
      if (tokenProviderEnabledConfig.findPrefixOf(key).isDefined) {
        val tokenProviderEnabledConfig(service) = key
        value.toBoolean || defaultTokenProviders.get(service).map(_._1).getOrElse(false)
      } else {
        false
      }
    }.map { case (key, _) =>
      val tokenProviderEnabledConfig(service) = key
      val cls = sparkConf.getOption(tokenProviderClsConfig.format(service))
        .orElse(defaultTokenProviders.get(service).map(_._2))
      (service, cls)
    }.foreach { case (service, cls) =>
      if (cls.isDefined) {
        try {
          val tokenProvider =
            Utils.classForName(cls.get).newInstance().asInstanceOf[ServiceTokenProvider]
          tokenProviders += (service -> tokenProvider)
        } catch {
          case NonFatal(e) =>
            logWarning(s"Fail to instantiate class ${cls.get}", e)
        }
      }
    }
  }

  def getServiceTokenProvider(service: String): Option[ServiceTokenProvider] = {
    tokenProviders.get(service)
  }

  def obtainTokensFromService(
      service: String,
      conf: Configuration,
      creds: Credentials): Array[Token[_]] = {
    val tokenBuf = mutable.ArrayBuffer[Token[_]]()
    getServiceTokenProvider(service).foreach { provider =>
      if (provider.isTokenRequired(conf)) {
        tokenBuf ++= provider.obtainTokensFromService(sparkConf, conf, creds)
      } else {
        logWarning(s"Service $service does not require a token. Check your configuration to see " +
          "if security is disabled or not.")
      }
    }

    tokenBuf.toArray
  }

  def obtainTokens(conf: Configuration, creds: Credentials): Array[Token[_]] = {
    val tokenBuf = mutable.ArrayBuffer[Token[_]]()
    tokenProviders.values.foreach { provider =>
      if (provider.isTokenRequired(conf)) {
        tokenBuf ++= provider.obtainTokensFromService(sparkConf, conf, creds)
      } else {
        logWarning(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
      }
    }

    tokenBuf.toArray
  }

  def getTokenRenewalIntervalFromService(service: String, conf: Configuration): Long = {
    getServiceTokenProvider(service).map { provider =>
      if (provider.isTokenRequired(conf) && provider.isInstanceOf[ServiceTokenRenewable]) {
        provider.asInstanceOf[ServiceTokenRenewable].getTokenRenewalInterval(sparkConf, conf)
      } else {
        Long.MaxValue
      }
    }.getOrElse(Long.MaxValue)
  }

  def getSmallestTokenRenewalInterval(conf: Configuration): Long = {
    tokenProviders.values.map { provider =>
      if (provider.isTokenRequired(conf) && provider.isInstanceOf[ServiceTokenRenewable]) {
        provider.asInstanceOf[ServiceTokenRenewable].getTokenRenewalInterval(sparkConf, conf)
      } else {
        Long.MaxValue
      }
    }.min
  }

  def getTimeFromNowToRenewalForService(
      service: String,
      conf: Configuration,
      fractional: Double,
      credentials: Credentials): Long = {
    getServiceTokenProvider(service).map { provider =>
      if (provider.isTokenRequired(conf) && provider.isInstanceOf[ServiceTokenRenewable]) {
        provider.asInstanceOf[ServiceTokenRenewable].getTimeFromNowToRenewal(
          sparkConf, fractional, credentials)
      } else {
        Long.MaxValue
      }
    }.getOrElse(Long.MaxValue)
  }

  def getNearestTimeFromNowToRenewal(
      conf: Configuration,
      fractional: Double,
      credentials: Credentials): Long = {
    tokenProviders.values.map { provider =>
      if (provider.isTokenRequired(conf) && provider.isInstanceOf[ServiceTokenRenewable]) {
        provider.asInstanceOf[ServiceTokenRenewable].getTimeFromNowToRenewal(
          sparkConf, fractional, credentials)
      } else {
        Long.MaxValue
      }
    }.min
  }

  def delegationTokenRenewer(conf: Configuration): AMDelegationTokenRenewer = synchronized {
    if (_delegationTokenRenewer == null) {
      _delegationTokenRenewer = new AMDelegationTokenRenewer(sparkConf, conf)
      _delegationTokenRenewer
    } else {
      _delegationTokenRenewer
    }
  }

  def delegationTokenUpdater(conf: Configuration): ExecutorDelegationTokenUpdater = synchronized {
    if (_delegationTokenUpdater == null) {
      _delegationTokenUpdater = new ExecutorDelegationTokenUpdater(sparkConf, conf)
      _delegationTokenUpdater
    } else {
      _delegationTokenUpdater
    }
  }

  def stop(): Unit = synchronized {
    if (_delegationTokenRenewer != null) {
      _delegationTokenRenewer.stop()
      _delegationTokenRenewer = null
    }

    if (_delegationTokenUpdater != null) {
      _delegationTokenUpdater.stop()
      _delegationTokenUpdater = null
    }
  }
}

private[yarn] object ConfigurableTokenManager {
  private var _configurableTokenManager: ConfigurableTokenManager = null

  def configurableTokenManager(conf: SparkConf): ConfigurableTokenManager = synchronized {
    if (_configurableTokenManager == null) {
      _configurableTokenManager = new ConfigurableTokenManager(conf)
      _configurableTokenManager.initialize()
      _configurableTokenManager
    } else {
      _configurableTokenManager
    }
  }

  /**
   * Get HDFS token provider, HDFS token provider requires special code to set NNs and token
   * renewer, so exposed here.
   */
  def hdfsTokenProvider(conf: SparkConf): HDFSTokenProvider = {
    configurableTokenManager(conf).getServiceTokenProvider("hdfs")
      .asInstanceOf[HDFSTokenProvider]
  }
}
