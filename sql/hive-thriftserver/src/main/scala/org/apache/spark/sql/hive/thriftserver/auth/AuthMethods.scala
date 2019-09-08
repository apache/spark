/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.auth

import javax.security.sasl.AuthenticationException

abstract class AuthMethods(authMethod: String) {
  def getAuthMethod: String = authMethod
}

object AuthMethods {
  @throws[AuthenticationException]
  def getValidAuthMethod(authMethodStr: String): AuthMethods = {
    for (auth <- AuthMethods.values) {
      if (authMethodStr == auth.getAuthMethod) {
        return auth
      }
    }
    throw new AuthenticationException("Not a valid authentication method")
  }

  def values: Seq[AuthMethods] = Seq(LDAP, PAM, CUSTOM, NONE)

  case object LDAP extends AuthMethods("LDAP")

  case object PAM extends AuthMethods("PAM")

  case object CUSTOM extends AuthMethods("CUSTOM")

  case object NONE extends AuthMethods("NONE")

}


