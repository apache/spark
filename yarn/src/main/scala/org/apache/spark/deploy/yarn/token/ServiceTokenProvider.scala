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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token

import org.apache.spark.SparkConf

/**
 * An interface to provide tokens for service, any service needs to be communicated with Spark
 * through token way need to implement this interface and register into
 * [[ConfigurableTokenManager]] through configurations.
 */
trait ServiceTokenProvider {

  /**
   * Name of the ServiceTokenProvider, should be unique. Using this to distinguish different
   * service.
   */
  def serviceName: String

  /**
   * Used to indicate whether a token is required.
   */
  def isTokenRequired(conf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  /**
   *  Obtain tokens from this service, tokens will be added into Credentials and return as array.
   */
  def obtainTokensFromService(
      sparkConf: SparkConf,
      serviceConf: Configuration,
      creds: Credentials): Array[Token[_]]
}

/**
 * An interface for service token which can be renewable, any [[ServiceTokenProvider]] in which
 * token can be renewable should also implement this interface, Spark's internal time-based
 * token renewal mechanism will invoke the methods to update the tokens periodically.
 */
trait ServiceTokenRenewable {
  /**
   * Get the token renewal interval from this service. This renewal interval will be used in
   * periodical token renewal mechanism.
   */
  def getTokenRenewalInterval(sparkConf: SparkConf, serviceConf: Configuration): Long

  /**
   * Get the time length from now to next renewal.
   */
  def getTimeFromNowToRenewal(
      sparkConf: SparkConf,
      fractional: Double,
      credentials: Credentials): Long
}
