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

package org.apache.spark.deploy.yarn.security

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A ConfigurableCredentialManager to manage all the registered credential providers and offer
 * APIs for other modules to obtain credentials as well as renewal time. By default
 * [[HDFSCredentialProvider]], [[HiveCredentialProvider]] and [[HBaseCredentialProvider]] will
 * be loaded in, any plugged-in credential provider wants to be managed by
 * ConfigurableCredentialManager needs to implement [[ServiceCredentialProvider]] interface and put
 * into resources to be loaded by ServiceLoader.
 *
 * Also the specific credential provider is controlled by
 * spark.yarn.security.credentials.{service}.enabled, it will not be loaded in if set to false.
 */
final class ConfigurableCredentialManager private[yarn] (sparkConf: SparkConf) extends Logging {
  private val deprecatedProviderEnabledConfig = "spark.yarn.security.tokens.%s.enabled"
  private val providerEnabledConfig = "spark.yarn.security.credentials.%s.enabled"

  // Maintain all the registered credential providers
  private val credentialProviders = mutable.HashMap[String, ServiceCredentialProvider]()

  // Default crendetial providers that will be loaded automatically, unless specifically disabled.
  private val defaultCredentialProviders = Map(
    "hdfs" -> "org.apache.spark.deploy.yarn.security.HDFSCredentialProvider",
    "hive" -> "org.apache.spark.deploy.yarn.security.HiveCredentialProvider",
    "hbase" -> "org.apache.spark.deploy.yarn.security.HBaseCredentialProvider"
  )

  // AMDelegationTokenRenewer, this will only be create and started in the AM
  private var _delegationTokenRenewer: AMDelegationTokenRenewer = null

  // ExecutorDelegationTokenUpdater, this will only be created and started in the driver and
  // executor side.
  private var _delegationTokenUpdater: ExecutorDelegationTokenUpdater = null

  def initialize(): Unit = {
    val providers = ServiceLoader.load(classOf[ServiceCredentialProvider],
      Utils.getContextOrSparkClassLoader).asScala

    // Filter out credentials in which spark.yarn.security.credentials.{service}.enabled is false.
    providers.filter { p =>
      sparkConf.getOption(providerEnabledConfig.format(p.serviceName))
        .orElse {
          logWarning(s"${deprecatedProviderEnabledConfig.format(p.serviceName)} is deprecated, " +
            s"using ${providerEnabledConfig.format(p.serviceName)} instead")
          sparkConf.getOption(deprecatedProviderEnabledConfig.format(p.serviceName))
        }
        .getOrElse(defaultCredentialProviders.keySet.find(_ == p.serviceName).isDefined.toString)
        .toBoolean
    }.foreach {
      p => credentialProviders(p.serviceName) = p
    }
  }

  /**
   * Get credential provider for the specified service.
   */
  def getServiceCredentialProvider(service: String): Option[ServiceCredentialProvider] = {
    credentialProviders.get(service)
  }

  /**
   * Obtain credentials for specified service.
   * @return time of next renewal if this service credential is renewable.
   */
  def obtainCredentialsFromService(
      service: String,
      hadoopConf: Configuration,
      creds: Credentials): Option[Long] = {
    getServiceCredentialProvider(service).flatMap { provider =>
      if (provider.isCredentialRequired(hadoopConf)) {
        provider.obtainCredentials(hadoopConf, creds)
      } else {
        None
      }
    }
  }

  /**
   * Obtain credentials from all the registered providers.
   * @return Array of time of next renewal if credential can be renewed.
   *         Otherwise will return None instead.
   */
  def obtainCredentials(hadoopConf: Configuration, creds: Credentials): Array[Option[Long]] = {
    val timeOfNextRenewals = mutable.ArrayBuffer[Option[Long]]()
    credentialProviders.values.foreach { provider =>
      if (provider.isCredentialRequired(hadoopConf)) {
        timeOfNextRenewals += provider.obtainCredentials(hadoopConf, creds)
      } else {
        logWarning(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
      }
    }

    timeOfNextRenewals.toArray
  }

  def delegationTokenRenewer(hadoopConf: Configuration): AMDelegationTokenRenewer = synchronized {
    if (_delegationTokenRenewer == null) {
      _delegationTokenRenewer = new AMDelegationTokenRenewer(sparkConf, hadoopConf, this)
      _delegationTokenRenewer
    } else {
      _delegationTokenRenewer
    }
  }

  def delegationTokenUpdater(hadoopConf: Configuration
      ): ExecutorDelegationTokenUpdater = synchronized {
    if (_delegationTokenUpdater == null) {
      _delegationTokenUpdater = new ExecutorDelegationTokenUpdater(sparkConf, hadoopConf, this)
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
