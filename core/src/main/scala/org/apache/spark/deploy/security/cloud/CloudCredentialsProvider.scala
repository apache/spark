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

package org.apache.spark.deploy.security.cloud

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi

/**
 * ::DeveloperApi::
 * Hadoop cloud credentials provider.
 */
@DeveloperApi
trait CloudCredentialsProvider {

  /**
   * Name of the service to provide credentials. This name should be unique.  Spark will
   * internally use this name to differentiate providers.
   */
  def serviceName: String

  /**
   * Obtain credentials for this service and get the time of the next renewal.
   * @param hadoopConf Configuration of current Hadoop Compatible system.
   * @return  a HadoopCloudCredentials object with the credentials and expiry time set
   */
  def obtainCredentials(
    hadoopConf: Configuration,
    sparkConf: SparkConf): Option[CloudCredentials]
}
