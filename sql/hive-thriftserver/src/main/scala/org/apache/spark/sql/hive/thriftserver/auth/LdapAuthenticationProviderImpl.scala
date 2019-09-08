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


import java.util.Hashtable
import javax.naming.{Context, NamingException}
import javax.naming.directory.InitialDirContext
import javax.security.sasl.AuthenticationException

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.sql.hive.thriftserver.ServiceUtils

private[auth] class LdapAuthenticationProviderImpl() extends PasswdAuthenticationProvider {

  final val conf = new HiveConf
  final private val ldapURL = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL)
  final private val baseDN = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN)
  final private val ldapDomain = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN)

  @throws[AuthenticationException]
  override def Authenticate(user: String, password: String): Unit = {
    var authUser = user
    val env = new Hashtable[String, AnyRef]
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.PROVIDER_URL, ldapURL)
    // If the domain is available in the config, then append it unless domain is
    // already part of the username. LDAP providers like Active Directory use a
    // fully qualified user name like foo@bar.com.
    if (!hasDomain(authUser) && ldapDomain != null) {
      authUser = authUser + "@" + ldapDomain
    }
    if (password == null || password.isEmpty || password.getBytes()(0) == 0) {
      throw new AuthenticationException("Error validating LDAP user:" +
        " a null or blank password has been provided")
    }
    // setup the security principal
    var bindDN: String = null
    if (baseDN == null) {
      bindDN = authUser
    } else bindDN = {
      "uid=" + authUser + "," + baseDN
    }
    env.put(Context.SECURITY_AUTHENTICATION, "simple")
    env.put(Context.SECURITY_PRINCIPAL, bindDN)
    env.put(Context.SECURITY_CREDENTIALS, password)
    try { // Create initial context
      val ctx = new InitialDirContext(env)
      ctx.close()
    } catch {
      case e: NamingException =>
        throw new AuthenticationException("Error validating LDAP user", e)
    }
  }

  private def hasDomain(userName: String): Boolean = ServiceUtils.indexOfDomainMatch(userName) > 0
}
