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

package org.apache.spark.deploy.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.Credentials

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * A ConfigurableCredentialManager to manage all the registered credential providers and offer
 * APIs for other modules to obtain credentials as well as renewal time. By default
 * [[HadoopFSCredentialProvider]], [[HiveCredentialProvider]] and [[HBaseCredentialProvider]] will
 * be loaded in if not explicitly disabled.
 *
 * Also each credential provider is controlled by spark.security.credentials.{service}.enabled,
 * it will not be loaded in if set to false.  For example, Hive's credential provider
 * [[HiveCredentialProvider]] can be enabled/disabled by the configuration
 * spark.security.credentials.hive.enabled.
 *
 * @param sparkConf Spark configuration
 * @param hadoopConf Hadoop configuration
 * @param fileSystems Delegation tokens will be fetched for these Hadoop filesystems.
 */
private[spark] class ConfigurableCredentialManager(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fileSystems: Set[FileSystem])
  extends Logging {

  private val deprecatedProviderEnabledConfigs = List(
    "spark.yarn.security.tokens.%s.enabled",
    "spark.yarn.security.credentials.%s.enabled")
  private val providerEnabledConfig = "spark.security.credentials.%s.enabled"

  // Maintain all the registered credential providers
  private val credentialProviders = getCredentialProviders
  logDebug(s"Using the following credential providers: ${credentialProviders.keys.mkString(", ")}.")

  private def getCredentialProviders: Map[String, HadoopDelegationTokenProvider] = {
    val providers = List(new HadoopFSCredentialProvider(fileSystems),
      new HiveCredentialProvider,
      new HBaseCredentialProvider)

    // Filter out credentials in which spark.security.credentials.{service}.enabled is false.
    providers
      .filter { p => isServiceEnabled(p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }

  def isServiceEnabled(serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)

    deprecatedProviderEnabledConfigs.foreach { pattern =>
      val deprecatedKey = pattern.format(serviceName)
      if (sparkConf.contains(deprecatedKey)) {
        logWarning(s"${deprecatedKey} is deprecated.  Please use ${key} instead.")
      }
    }

    val isEnabledDeprecated = deprecatedProviderEnabledConfigs.forall { pattern =>
      sparkConf
        .getOption(pattern.format(serviceName))
        .map(_.toBoolean)
        .getOrElse(true)
    }

    sparkConf
      .getOption(key)
      .map(_.toBoolean)
      .getOrElse(isEnabledDeprecated)
  }

  /**
   * Get credential provider for the specified service.
   */
  def getServiceCredentialProvider(service: String): Option[HadoopDelegationTokenProvider] = {
    credentialProviders.get(service)
  }

  /**
   * Writes delegation tokens to creds.  Delegation tokens are fetched from all registered
   * providers.
   *
   * @return Time after which the fetched delegation tokens should be renewed.
   */
  def obtainCredentials(
      hadoopConf: Configuration,
      creds: Credentials): Long = {
    credentialProviders.values.flatMap { provider =>
      if (provider.credentialsRequired(hadoopConf)) {
        provider.obtainCredentials(hadoopConf, creds)
      } else {
        logDebug(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
        None
      }
    }.foldLeft(Long.MaxValue)(math.min)
  }
}
