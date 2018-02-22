/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth.ldap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Static utility methods related to LDAP authentication module.
 */
public final class LdapUtils {

  private static final Logger LOG = LoggerFactory.getLogger(LdapUtils.class);

  /**
   * Extracts a base DN from the provided distinguished name.
   * <br>
   * <b>Example:</b>
   * <br>
   * "ou=CORP,dc=mycompany,dc=com" is the base DN for "cn=user1,ou=CORP,dc=mycompany,dc=com"
   *
   * @param dn distinguished name
   * @return base DN
   */
  public static String extractBaseDn(String dn) {
    final int indexOfFirstDelimiter = dn.indexOf(",");
    if (indexOfFirstDelimiter > -1) {
      return dn.substring(indexOfFirstDelimiter + 1);
    }
    return null;
  }

  /**
   * Extracts the first Relative Distinguished Name (RDN).
   * <br>
   * <b>Example:</b>
   * <br>
   * For DN "cn=user1,ou=CORP,dc=mycompany,dc=com" this method will return "cn=user1"
   * @param dn distinguished name
   * @return first RDN
   */
  public static String extractFirstRdn(String dn) {
    return dn.substring(0, dn.indexOf(","));
  }

  /**
   * Extracts username from user DN.
   * <br>
   * <b>Examples:</b>
   * <pre>
   * LdapUtils.extractUserName("UserName")                        = "UserName"
   * LdapUtils.extractUserName("UserName@mycorp.com")             = "UserName"
   * LdapUtils.extractUserName("cn=UserName,dc=mycompany,dc=com") = "UserName"
   * </pre>
   * @param userDn
   * @return
   */
  public static String extractUserName(String userDn) {
    if (!isDn(userDn) && !hasDomain(userDn)) {
      return userDn;
    }

    int domainIdx = ServiceUtils.indexOfDomainMatch(userDn);
    if (domainIdx > 0) {
      return userDn.substring(0, domainIdx);
    }

    if (userDn.contains("=")) {
      return userDn.substring(userDn.indexOf("=") + 1, userDn.indexOf(","));
    }
    return userDn;
  }

  /**
   * Gets value part of the first attribute in the provided RDN.
   * <br>
   * <b>Example:</b>
   * <br>
   * For RDN "cn=user1,ou=CORP" this method will return "user1"
   * @param rdn Relative Distinguished Name
   * @return value part of the first attribute
   */
  public static String getShortName(String rdn) {
    return ((rdn.split(","))[0].split("="))[1];
  }

  /**
   * Check for a domain part in the provided username.
   * <br>
   * <b>Example:</b>
   * <br>
   * <pre>
   * LdapUtils.hasDomain("user1@mycorp.com") = true
   * LdapUtils.hasDomain("user1")            = false
   * </pre>
   * @param userName username
   * @return true if {@code userName} contains {@code @<domain>} part
   */
  public static boolean hasDomain(String userName) {
    return (ServiceUtils.indexOfDomainMatch(userName) > 0);
  }

  /**
   * Detects DN names.
   * <br>
   * <b>Example:</b>
   * <br>
   * <pre>
   * LdapUtils.isDn("cn=UserName,dc=mycompany,dc=com") = true
   * LdapUtils.isDn("user1")                           = false
   * </pre>
   * @param name name to be checked
   * @return true if the provided name is a distinguished name
   */
  public static boolean isDn(String name) {
    return name.contains("=");
  }

  /**
   * Reads and parses DN patterns from Hive configuration.
   * <br>
   * If no patterns are provided in the configuration, then the base DN will be used.
   * @param conf Hive configuration
   * @param var variable to be read
   * @return a list of DN patterns
   * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN
   * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GUIDKEY
   * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN
   * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN
   */
  public static List<String> parseDnPatterns(HiveConf conf, HiveConf.ConfVars var) {
    String patternsString = conf.getVar(var);
    List<String> result = new ArrayList<>();
    if (StringUtils.isBlank(patternsString)) {
      String defaultBaseDn = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN);
      String guidAttr = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GUIDKEY);
      if (StringUtils.isNotBlank(defaultBaseDn)) {
        result.add(guidAttr + "=%s," + defaultBaseDn);
      }
    } else {
      String[] patterns = patternsString.split(":");
      for (String pattern : patterns) {
        if (pattern.contains(",") && pattern.contains("=")) {
          result.add(pattern);
        } else {
          LOG.warn("Unexpected format for " + var + "..ignoring " + pattern);
        }
      }
    }
    return result;
  }

  private static String patternToBaseDn(String pattern) {
    if (pattern.contains("=%s")) {
      return pattern.split(",", 2)[1];
    }
    return pattern;
  }

  /**
   * Converts a collection of Distinguished Name patterns to a collection of base DNs.
   * @param patterns Distinguished Name patterns
   * @return a list of base DNs
   * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN
   * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN
   */
  public static List<String> patternsToBaseDns(Collection<String> patterns) {
    List<String> result = new ArrayList<>();
    for (String pattern : patterns) {
      result.add(patternToBaseDn(pattern));
    }
    return result;
  }

  /**
   * Creates a list of principals to be used for user authentication.
   * @param conf Hive configuration
   * @param user username
   * @return a list of user's principals
   */
  public static List<String> createCandidatePrincipals(HiveConf conf, String user) {
    if (hasDomain(user) || isDn(user)) {
      return Collections.singletonList(user);
    }

    String ldapDomain = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN);
    if (StringUtils.isNotBlank(ldapDomain)) {
      return Collections.singletonList(user + "@" + ldapDomain);
    }

    List<String> userPatterns = parseDnPatterns(conf,
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN);
    if (userPatterns.isEmpty()) {
      return Collections.singletonList(user);
    }

    List<String> candidatePrincipals = new ArrayList<>();
    for (String userPattern : userPatterns) {
      candidatePrincipals.add(userPattern.replaceAll("%s", user));
    }
    return candidatePrincipals;
  }

  private LdapUtils() {
  }
}
