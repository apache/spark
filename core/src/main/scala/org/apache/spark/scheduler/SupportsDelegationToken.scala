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
import org.apache.spark.deploy.security.{HadoopDelegationTokenManager, UserCredentialManager}

/**
 * A mix-in trait for SchedulerBackend that supports delegation tokens.
 */
private[spark] trait SupportsDelegationToken {

  // The token manager used to create security tokens.
  protected var delegationTokenManager: Option[HadoopDelegationTokenManager] = None

  // The OIDC user-credential manager, a sibling of the Kerberos delegation token manager.
  // Its lifecycle lives here (like delegationTokenManager) so that both
  // CoarseGrainedSchedulerBackend and LocalSchedulerBackend share a single implementation; the
  // two backends differ only in how they propagate credentials, expressed via
  // propagateUserCredentials().
  protected var userCredentialManager: Option[UserCredentialManager] = None

  /**
   * The task scheduler this backend belongs to. Implemented by the mixing-in backend (both
   * CoarseGrainedSchedulerBackend and LocalSchedulerBackend already hold a `scheduler` field).
   * Used to reach `scheduler.sc.conf` / `scheduler.sc.env` / the OIDC credential provider loader.
   */
  protected def scheduler: TaskSchedulerImpl

  /**
   * Create the delegation token manager to be used for the application. This method is called
   * once during the start of the scheduler backend (so after the object has already been
   * fully constructed), when security is enabled or direct credential providers are configured.
   */
  protected def createTokenManager(): Option[HadoopDelegationTokenManager] = None

  /**
   * Called when a new set of delegation tokens is sent to the driver.
   */
  protected def updateDelegationTokens(tokens: Array[Byte]): Unit

  /**
   * Propagate a freshly acquired set of OIDC user credentials. Called on the driver by the
   * [[UserCredentialManager]] (initially from `start()`, then on each renewal). Implemented per
   * backend: `CoarseGrainedSchedulerBackend` updates the driver store and broadcasts to executors
   * via its `DriverEndpoint`; `LocalSchedulerBackend` (no remote executors) updates the shared
   * credential store directly.
   */
  protected def propagateUserCredentials(version: Long, credentials: Array[Byte]): Unit

  /**
   * Whether the token manager should be started. The default implementation returns true when
   * Hadoop security is enabled. Backends that support direct credential providers override this
   * method to also check whether those providers are configured.
   */
  protected def tokenManagerRequired(): Boolean = UserGroupInformation.isSecurityEnabled

  protected def setupTokenManager(): Unit = {
    if (tokenManagerRequired()) {
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

  /**
   * Start the [[UserCredentialManager]] if OIDC credential propagation is enabled. Called once
   * during scheduler-backend start, independently of the Kerberos delegation token manager.
   *
   * Binds the manager to `scheduler.sc.conf` (the live SparkConf, not a clone) so the
   * resolution-time fallback in `UserCredentialManager.start()` -- which applies provider-declared
   * `spark.*` properties for any scheme the selection phase could not -- reaches the same conf on
   * both backends. Reuses the [[org.apache.spark.security.CredentialProviderLoader]] produced by
   * SparkContext's selection phase so providers are initialized exactly once; SparkContext remains
   * the loader's single owner. `start()` invokes `propagateUserCredentials` synchronously for the
   * initial credentials, so no separate initial store is needed here.
   */
  protected def setupUserCredentialManager(): Unit = {
    userCredentialManager = UserCredentialManager.create(
      scheduler.sc.conf, propagateUserCredentials, scheduler.sc.userCredentialProviderLoader)
    userCredentialManager.foreach(_.start())
  }

  protected def stopUserCredentialManager(): Unit = {
    userCredentialManager.foreach(_.stop())
  }
}
