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
package org.apache.hive.service.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.security.sasl.AuthenticationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.ServiceUtils;

public class LdapAuthenticationProviderImpl implements PasswdAuthenticationProvider {

  private final String ldapURL;
  private final String baseDN;
  private final String ldapDomain;
  private final String userDNPattern;

  LdapAuthenticationProviderImpl() {
    HiveConf conf = new HiveConf();
    ldapURL = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL);
    baseDN = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN);
    ldapDomain = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN);
    userDNPattern = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN);
  }

  @Override
  public void Authenticate(String user, String password) throws AuthenticationException {

    // If the domain is available in the config, then append it unless domain is
    // already part of the username. LDAP providers like Active Directory use a
    // fully qualified user name like foo@bar.com.
    if (!hasDomain(user) && ldapDomain != null) {
      user  = user + "@" + ldapDomain;
    }

    if (password == null || password.isEmpty() || password.getBytes()[0] == 0) {
      throw new AuthenticationException("Error validating LDAP user:" +
          " a null or blank password has been provided");
    }

    // setup the security principal
    List<String> candidatePrincipals = new ArrayList<>();
    if (StringUtils.isBlank(userDNPattern)) {
      if (StringUtils.isNotBlank(baseDN)) {
        String pattern = "uid=" + user + "," + baseDN;
        candidatePrincipals.add(pattern);
      }
    } else {
      String[] patterns = userDNPattern.split(":");
      for (String pattern : patterns) {
        if (StringUtils.contains(pattern, ",") && StringUtils.contains(pattern, "=")) {
          candidatePrincipals.add(pattern.replaceAll("%s", user));
        }
      }
    }

    if (candidatePrincipals.isEmpty()) {
      candidatePrincipals = Collections.singletonList(user);
    }

    for (Iterator<String> iterator = candidatePrincipals.iterator(); iterator.hasNext();) {
      String principal = iterator.next();

      Hashtable<String, Object> env = new Hashtable<String, Object>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.PROVIDER_URL, ldapURL);
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, principal);
      env.put(Context.SECURITY_CREDENTIALS, password);

      try {

        // Create initial context
        Context ctx = new InitialDirContext(env);
        ctx.close();
        break;
      } catch (NamingException e) {
        if (!iterator.hasNext()) {
          throw new AuthenticationException("Error validating LDAP user", e);
        }
      }
    }
  }

  private boolean hasDomain(String userName) {
    return (ServiceUtils.indexOfDomainMatch(userName) > 0);
  }
}
