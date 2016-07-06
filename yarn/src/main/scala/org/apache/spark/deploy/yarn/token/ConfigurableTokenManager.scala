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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A [[ConfigurableTokenManager]] to manage all the token providers register in this class. Also
 * it provides other modules the functionality to obtain tokens, get token renweal interval and
 * calculate the time length till next renewal.
 *
 * By default ConfigurableTokenManager has 3 built-in token providers, HDFSTokenProvider,
 * HiveTokenProvider and HBaseTokenProvider, and this 3 token providers can also be controlled
 * by configuration spark.yarn.security.tokens.{service}.enabled, if it is set to false, this
 * provider will not be loaded.
 *
 * For other token providers which need to be loaded in should:
 * 1. Implement [[ServiceTokenProvider]] and [[ServiceTokenRenewable]] if token renewal is
 * required for this service.
 * 2. set spark.yarn.security.tokens.{service}.enabled to true
 * 3. Specify the class name through spark.yarn.security.tokens.{service}.class
 *
 */
final class ConfigurableTokenManager private[yarn] (sparkConf: SparkConf) extends Logging {
  private val tokenProviderEnabledConfig = "spark\\.yarn\\.security\\.tokens\\.(.+)\\.enabled".r
  private val tokenProviderClsConfig = "spark.yarn.security.tokens.%s.class"

  // Maintain all the registered token providers
  private val tokenProviders = mutable.HashMap[String, ServiceTokenProvider]()

  private val defaultTokenProviders = Map(
    "hdfs" -> "org.apache.spark.deploy.yarn.token.HDFSTokenProvider",
    "hive" -> "org.apache.spark.deploy.yarn.token.HiveTokenProvider",
    "hbase" -> "org.apache.spark.deploy.yarn.token.HBaseTokenProvider"
  )

  // AMDelegationTokenRenewer, this will only be create and started in the AM
  private var _delegationTokenRenewer: AMDelegationTokenRenewer = null

  // ExecutorDelegationTokenUpdater, this will only be created and started in the driver and
  // executor side.
  private var _delegationTokenUpdater: ExecutorDelegationTokenUpdater = null

  def initialize(): Unit = {
    // Copy SparkConf and add default enabled token provider configurations to SparkConf.
    val clonedConf = sparkConf.clone
    defaultTokenProviders.keys.foreach { key =>
      clonedConf.setIfMissing(s"spark.yarn.security.tokens.$key.enabled", "true")
    }

    // Instantialize all the service token providers according to the configurations.
    clonedConf.getAll.filter { case (key, value) =>
      if (tokenProviderEnabledConfig.findPrefixOf(key).isDefined) {
        value.toBoolean
      } else {
        false
      }
    }.map { case (key, _) =>
      val tokenProviderEnabledConfig(service) = key
      val cls = sparkConf.getOption(tokenProviderClsConfig.format(service))
        .orElse(defaultTokenProviders.get(service))
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

  def getSmallestTokenRenewalInterval(conf: Configuration): Long = {
    tokenProviders.values.map { provider =>
      if (provider.isTokenRequired(conf) && provider.isInstanceOf[ServiceTokenRenewable]) {
        provider.asInstanceOf[ServiceTokenRenewable].getTokenRenewalInterval(sparkConf, conf)
      } else {
        Long.MaxValue
      }
    }.min
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

    if (ConfigurableTokenManager._configurableTokenManager != null) {
      ConfigurableTokenManager._configurableTokenManager = null
    }
  }
}

object ConfigurableTokenManager {
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
      .map(_.asInstanceOf[HDFSTokenProvider])
      .getOrElse(throw new SparkException("Failed to load HDFS token provider"))
  }
}
