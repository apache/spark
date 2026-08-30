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
package org.apache.hive.service.auth

import java.util.{Arrays => JArrays, List => JList}

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.SparkFunSuite

class LdapAuthenticationProviderImplSuite extends SparkFunSuite {

  private def newProvider(
      baseDN: String = null,
      userDNPattern: String = null,
      ldapDomain: String = null): LdapAuthenticationProviderImpl = {
    val conf = new HiveConf()
    // Avoid picking up a real LDAP URL from any hive-site.xml on the test classpath.
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL, "ldap://localhost:389")
    if (baseDN != null) {
      conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN, baseDN)
    }
    if (userDNPattern != null) {
      conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN, userDNPattern)
    }
    if (ldapDomain != null) {
      conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN, ldapDomain)
    }
    new LdapAuthenticationProviderImpl(conf)
  }

  private def principals(provider: LdapAuthenticationProviderImpl, user: String): JList[String] =
    provider.createCandidatePrincipals(user)

  private def dns(values: String*): JList[String] = JArrays.asList(values: _*)

  test("legitimate usernames are left untouched when building the DN") {
    val provider = newProvider(baseDN = "ou=users,dc=example,dc=com")
    assert(principals(provider, "alice") ===
      dns("uid=alice,ou=users,dc=example,dc=com"))
  }

  test("LDAP special characters in the username are escaped for the baseDN pattern") {
    val provider = newProvider(baseDN = "ou=users,dc=example,dc=com")
    // Without escaping this would be "uid=admin,ou=Admins,ou=users,dc=example,dc=com", letting
    // the attacker bind as a different DN (authentication bypass / impersonation).
    assert(principals(provider, "admin,ou=Admins") ===
      dns("uid=admin\\,ou\\=Admins,ou=users,dc=example,dc=com"))
  }

  test("full DN structure manipulation is neutralized for the baseDN pattern") {
    val provider = newProvider(baseDN = "ou=users,dc=example,dc=com")
    assert(principals(provider, "attacker,ou=Admins,dc=example,dc=com") ===
      dns("uid=attacker\\,ou\\=Admins\\,dc\\=example\\,dc\\=com,ou=users,dc=example,dc=com"))
  }

  test("LDAP special characters in the username are escaped for userDNPattern") {
    val provider = newProvider(userDNPattern = "uid=%s,ou=service,dc=example,dc=com")
    assert(principals(provider, "admin,ou=Admins") ===
      dns("uid=admin\\,ou\\=Admins,ou=service,dc=example,dc=com"))
  }

  test("multiple userDNPatterns are all escaped") {
    val provider = newProvider(
      userDNPattern = "uid=%s,ou=service,dc=example,dc=com:cn=%s,ou=people,dc=example,dc=com")
    assert(principals(provider, "a+b,c") === dns(
      "uid=a\\+b\\,c,ou=service,dc=example,dc=com",
      "cn=a\\+b\\,c,ou=people,dc=example,dc=com"))
  }

  test("escaped backslashes are inserted literally (no regex replacement)") {
    // The escaped value contains a backslash; String.replaceAll would treat it as a regex escape
    // and either throw or corrupt the output. Verify literal insertion.
    val provider = newProvider(userDNPattern = "uid=%s,ou=service,dc=example,dc=com")
    assert(principals(provider, "a\\b") ===
      dns("uid=a\\\\b,ou=service,dc=example,dc=com"))
  }

  test("leading and trailing spaces in the username are escaped") {
    val provider = newProvider(baseDN = "ou=users,dc=example,dc=com")
    assert(principals(provider, " alice ") ===
      dns("uid=\\ alice\\ ,ou=users,dc=example,dc=com"))
  }
}
