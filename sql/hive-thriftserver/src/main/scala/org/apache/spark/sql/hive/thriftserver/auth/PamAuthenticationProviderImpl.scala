/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.auth

import javax.security.sasl.AuthenticationException

import net.sf.jpam.Pam
import org.apache.hadoop.hive.conf.HiveConf

private[auth] class PamAuthenticationProviderImpl extends PasswdAuthenticationProvider {
  val conf = new HiveConf
  final private var pamServiceNames: String =
    conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PAM_SERVICES)

  @throws[AuthenticationException]
  override def Authenticate(user: String, password: String): Unit = {
    if (pamServiceNames == null || pamServiceNames.trim.isEmpty) {
      throw new AuthenticationException("No PAM services are set.")
    }
    val pamServices = pamServiceNames.split(",")
    for (pamService <- pamServices) {
      val pam = new Pam(pamService)
      val isAuthenticated = pam.authenticateSuccessful(user, password)
      if (!isAuthenticated) {
        throw new AuthenticationException("Error authenticating with the PAM service: " +
          pamService)
      }
    }
  }
}
