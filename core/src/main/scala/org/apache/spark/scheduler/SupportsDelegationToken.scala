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

package org.apache.spark.scheduler

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager

/**
 * A mix-in trait for SchedulerBackend that supports delegation tokens.
 */
private[spark] trait SupportsDelegationToken {

  // The token manager used to create security tokens.
  protected var delegationTokenManager: Option[HadoopDelegationTokenManager] = None

  /**
   * Create the delegation token manager to be used for the application. This method is called
   * once during the start of the scheduler backend (so after the object has already been
   * fully constructed), only if security is enabled in the Hadoop configuration.
   */
  protected def createTokenManager(): Option[HadoopDelegationTokenManager] = None

  /**
   * Called when a new set of delegation tokens is sent to the driver.
   */
  protected def updateDelegationTokens(tokens: Array[Byte]): Unit

  protected def setupTokenManager(): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      delegationTokenManager = createTokenManager()
      delegationTokenManager.foreach { dtm =>
        val ugi = UserGroupInformation.getCurrentUser
        val tokens = if (dtm.renewalEnabled) {
          dtm.start()
        } else {
          val creds = ugi.getCredentials
          dtm.obtainDelegationTokens(creds)
          if (creds.numberOfTokens() > 0 || creds.numberOfSecretKeys() > 0) {
            SparkHadoopUtil.get.serialize(creds)
          } else {
            null
          }
        }
        if (tokens != null) {
          updateDelegationTokens(tokens)
        }
      }
    }
  }

  protected def stopTokenManager(): Unit = {
    delegationTokenManager.foreach(_.stop())
  }
}
