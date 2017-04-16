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
package org.apache.hadoop.security.authentication.client;

import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.junit.Ignore;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.Callable;

//Disabled because kerberos setup and valid keytabs are required.
@Ignore("requires kerberos setup")
public class TestKerberosAuthenticator extends AuthenticatorTestCase {

  private Properties getAuthenticationHandlerConfiguration() {
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "kerberos");
    props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    props.setProperty(KerberosAuthenticationHandler.KEYTAB, KerberosTestUtils.getKeytabFile());
    props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
                      "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm()+")s/@.*//\n");
    return props;
  }

  public void testFallbacktoPseudoAuthenticator() throws Exception {
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
    props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
    setAuthenticationHandlerConfig(props);
    _testAuthentication(new KerberosAuthenticator(), false);
  }

  public void testNotAuthenticated() throws Exception {
    setAuthenticationHandlerConfig(getAuthenticationHandlerConfiguration());
    start();
    try {
      URL url = new URL(getBaseURL());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
      assertTrue(conn.getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE) != null);
    } finally {
      stop();
    }
  }


  public void testAuthentication() throws Exception {
    setAuthenticationHandlerConfig(getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        _testAuthentication(new KerberosAuthenticator(), false);
        return null;
      }
    });
  }

  public void testAuthenticationPost() throws Exception {
    setAuthenticationHandlerConfig(getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        _testAuthentication(new KerberosAuthenticator(), true);
        return null;
      }
    });
  }

}
