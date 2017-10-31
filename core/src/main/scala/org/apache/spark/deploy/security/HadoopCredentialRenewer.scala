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

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import org.apache.spark.internal.Logging

/**
 * Abstract class for credential renewers used my YARN and Mesos. To renew delegation tokens
 * the scheduler backend calls [[scheduleTokenRenewal]]. The implementation of this method
 * (and the dispersal of tokens) is resource-manager-specific, see implementations of this
 * class for details.
 */
abstract class HadoopCredentialRenewer extends Logging {

  protected val credentialRenewerThread: ScheduledExecutorService

  protected var timeOfNextRenewal: Long

  def scheduleTokenRenewal: Unit

  /**
   * Schedule re-login and creation of new credentials. If credentials have already expired, this
   * method will synchronously create new ones.
   */
  protected def scheduleRenewal(runnable: Runnable): Unit = {
    val remainingTime = timeOfNextRenewal - System.currentTimeMillis()
    if (remainingTime <= 0) {
      logInfo("Credentials have expired, creating new ones now.")
      runnable.run()
    } else {
      logInfo(s"Scheduling login from keytab in $remainingTime millis.")
      credentialRenewerThread.schedule(runnable, remainingTime, TimeUnit.MILLISECONDS)
    }
  }

  protected def getTimeOfNextUpdate(nearestRenewalTime: Long, factor: Double): Long = {
    val ct = System.currentTimeMillis()
    (ct + (factor * (nearestRenewalTime - ct))).toLong
  }
}
