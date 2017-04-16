/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication;

import com.sun.security.auth.module.Krb5LoginModule;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.io.File;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Test helper class for Java Kerberos setup.
 */
public class KerberosTestUtils {
  private static final String PREFIX = "hadoop-auth.test.";

  public static final String REALM = PREFIX + "kerberos.realm";

  public static final String CLIENT_PRINCIPAL = PREFIX + "kerberos.client.principal";

  public static final String SERVER_PRINCIPAL = PREFIX + "kerberos.server.principal";

  public static final String KEYTAB_FILE = PREFIX + "kerberos.keytab.file";

  public static String getRealm() {
    return System.getProperty(REALM, "LOCALHOST");
  }

  public static String getClientPrincipal() {
    return System.getProperty(CLIENT_PRINCIPAL, "client") + "@" + getRealm();
  }

  public static String getServerPrincipal() {
    return System.getProperty(SERVER_PRINCIPAL, "HTTP/localhost") + "@" + getRealm();
  }

  public static String getKeytabFile() {
    String keytabFile =
      new File(System.getProperty("user.home"), System.getProperty("user.name") + ".keytab").toString();
    return System.getProperty(KEYTAB_FILE, keytabFile);
  }

  private static class KerberosConfiguration extends Configuration {
    private String principal;

    public KerberosConfiguration(String principal) {
      this.principal = principal;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", KerberosTestUtils.getKeytabFile());
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[]{
        new AppConfigurationEntry(Krb5LoginModule.class.getName(),
                                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                  options),};
    }
  }

  public static <T> T doAs(String principal, final Callable<T> callable) throws Exception {
    LoginContext loginContext = null;
    try {
      Set<Principal> principals = new HashSet<Principal>();
      principals.add(new KerberosPrincipal(KerberosTestUtils.getClientPrincipal()));
      Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
      loginContext = new LoginContext("", subject, null, new KerberosConfiguration(principal));
      loginContext.login();
      subject = loginContext.getSubject();
      return Subject.doAs(subject, new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return callable.call();
        }
      });
    } catch (PrivilegedActionException ex) {
      throw ex.getException();
    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
    }
  }

  public static <T> T doAsClient(Callable<T> callable) throws Exception {
    return doAs(getClientPrincipal(), callable);
  }

  public static <T> T doAsServer(Callable<T> callable) throws Exception {
    return doAs(getServerPrincipal(), callable);
  }

}
