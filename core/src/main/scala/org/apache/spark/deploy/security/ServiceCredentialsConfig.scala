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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Helper class to define and access configuration parameters for ServiceCredentialsProviders
 */
private[spark] trait ServiceCredentialsConfig extends Logging{

  /**
   * Configuration param for enabling the credentials provider
   * @return a configuration string
   */
  def providerEnabledConfig : String
  /**
   * Deprecated configuration params for enabling the credentials provider
   * @return a list of configuration strings. May be empty
   */
  def deprecatedProviderEnabledConfigs : List[String]

  def isServiceEnabled(sparkConf: SparkConf, serviceName: String): Boolean = {
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

}


