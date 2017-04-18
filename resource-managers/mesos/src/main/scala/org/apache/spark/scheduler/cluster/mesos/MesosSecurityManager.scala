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

package org.apache.spark.scheduler.cluster.mesos

import javax.xml.bind.DatatypeConverter

import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.mesos.config
import org.apache.spark.deploy.security.ConfigurableCredentialManager
import org.apache.spark.internal.Logging


private[mesos] class MesosSecurityManager extends Logging {
  def isSecurityEnabled(): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  /** Writes delegation tokens to spark.mesos.kerberos.ugiTokens */
  def setUGITokens(conf: SparkConf): Unit = {
    val userCreds = getDelegationTokens(conf)

    val byteStream = new java.io.ByteArrayOutputStream(1024 * 1024)
    val dataStream = new java.io.DataOutputStream(byteStream)
    userCreds.writeTokenStorageToStream(dataStream)
    val credsBytes = byteStream.toByteArray

    logInfo(s"Writing ${credsBytes.length} bytes to ${config.USER_CREDENTIALS.key}.")

    val creds64 = DatatypeConverter.printBase64Binary(credsBytes)
    conf.set(config.USER_CREDENTIALS, creds64)
  }

  /**
   * Returns the user's credentials, with new delegation tokens added for all configured
   * services.
   */
  private def getDelegationTokens(conf: SparkConf): Credentials = {
    val userCreds = UserGroupInformation.getCurrentUser.getCredentials
    val numTokensBefore = userCreds.numberOfTokens
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val credentialManager = new ConfigurableCredentialManager(conf, hadoopConf)
    credentialManager.obtainCredentials(hadoopConf, userCreds)

    logDebug(s"Fetched ${userCreds.numberOfTokens - numTokensBefore} delegation token(s).")

    userCreds
  }
}
