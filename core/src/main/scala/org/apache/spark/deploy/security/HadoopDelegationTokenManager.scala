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
 * Manages all the registered HadoopDelegationTokenProviders and offer APIs for other modules to
 * obtain delegation tokens and their renewal time. By default [[HadoopFSDelegationTokenProvider]],
 * [[HiveDelegationTokenProvider]] and [[HBaseDelegationTokenProvider]] will be loaded in if not
 * explicitly disabled.
 *
 * Also, each HadoopDelegationTokenProvider is controlled by
 * spark.security.credentials.{service}.enabled, and will not be loaded if this config is set to
 * false. For example, Hive's delegation token provider [[HiveDelegationTokenProvider]] can be
 * enabled/disabled by the configuration spark.security.credentials.hive.enabled.
 *
 * @param sparkConf Spark configuration
 * @param hadoopConf Hadoop configuration
 * @param fileSystems Delegation tokens will be fetched for these Hadoop filesystems.
 */
private[spark] class HadoopDelegationTokenManager(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fileSystems: Configuration => Set[FileSystem])
  extends Logging {

  private val deprecatedProviderEnabledConfigs = List(
    "spark.yarn.security.tokens.%s.enabled",
    "spark.yarn.security.credentials.%s.enabled")
  private val providerEnabledConfig = "spark.security.credentials.%s.enabled"

  // Maintain all the registered delegation token providers
  private val delegationTokenProviders = getDelegationTokenProviders
  logDebug("Using the following builtin delegation token providers: " +
    s"${delegationTokenProviders.keys.mkString(", ")}.")

  /** Construct a [[HadoopDelegationTokenManager]] for the default Hadoop filesystem */
  def this(sparkConf: SparkConf, hadoopConf: Configuration) = {
    this(
      sparkConf,
      hadoopConf,
      hadoopConf => Set(FileSystem.get(hadoopConf).getHomeDirectory.getFileSystem(hadoopConf)))
  }

  private def getDelegationTokenProviders: Map[String, HadoopDelegationTokenProvider] = {
    val providers = Seq(new HadoopFSDelegationTokenProvider(fileSystems)) ++
      safeCreateProvider(new HiveDelegationTokenProvider) ++
      safeCreateProvider(new HBaseDelegationTokenProvider)

    // Filter out providers for which spark.security.credentials.{service}.enabled is false.
    providers
      .filter { p => isServiceEnabled(p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }

  private def safeCreateProvider(
      createFn: => HadoopDelegationTokenProvider): Option[HadoopDelegationTokenProvider] = {
    try {
      Some(createFn)
    } catch {
      case t: Throwable =>
        logDebug(s"Failed to load built in provider.", t)
        None
    }
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
   * Get delegation token provider for the specified service.
   */
  def getServiceDelegationTokenProvider(service: String): Option[HadoopDelegationTokenProvider] = {
    delegationTokenProviders.get(service)
  }

  /**
   * Writes delegation tokens to creds.  Delegation tokens are fetched from all registered
   * providers.
   *
   * @param hadoopConf hadoop Configuration
   * @param creds Credentials that will be updated in place (overwritten)
   * @return Time after which the fetched delegation tokens should be renewed.
   */
  def obtainDelegationTokens(
      hadoopConf: Configuration,
      creds: Credentials): Long = {
    delegationTokenProviders.values.flatMap { provider =>
      if (provider.delegationTokensRequired(sparkConf, hadoopConf)) {
        provider.obtainDelegationTokens(hadoopConf, sparkConf, creds)
      } else {
        logDebug(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
        None
      }
    }.foldLeft(Long.MaxValue)(math.min)
  }
}

