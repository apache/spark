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

import com.sun.security.auth.module.Krb5LoginModule;
import org.apache.commons.codec.binary.Base64;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import sun.security.jgss.GSSUtil;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link KerberosAuthenticator} implements the Kerberos SPNEGO authentication sequence.
 * <p/>
 * It uses the default principal for the Kerberos cache (normally set via kinit).
 * <p/>
 * It falls back to the {@link PseudoAuthenticator} if the HTTP endpoint does not trigger an SPNEGO authentication
 * sequence.
 */
public class KerberosAuthenticator implements Authenticator {

  /**
   * HTTP header used by the SPNEGO server endpoint during an authentication sequence.
   */
  public static final String WWW_AUTHENTICATE = "WWW-Authenticate";

  /**
   * HTTP header used by the SPNEGO client endpoint during an authentication sequence.
   */
  public static final String AUTHORIZATION = "Authorization";

  /**
   * HTTP header prefix used by the SPNEGO client/server endpoints during an authentication sequence.
   */
  public static final String NEGOTIATE = "Negotiate";

  private static final String AUTH_HTTP_METHOD = "OPTIONS";

  /*
  * Defines the Kerberos configuration that will be used to obtain the Kerberos principal from the
  * Kerberos cache.
  */
  private static class KerberosConfiguration extends Configuration {

    private static final String OS_LOGIN_MODULE_NAME;
    private static final boolean windows = System.getProperty("os.name").startsWith("Windows");

    static {
      if (windows) {
        OS_LOGIN_MODULE_NAME = "com.sun.security.auth.module.NTLoginModule";
      } else {
        OS_LOGIN_MODULE_NAME = "com.sun.security.auth.module.UnixLoginModule";
      }
    }

    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
      new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                new HashMap<String, String>());

    private static final Map<String, String> USER_KERBEROS_OPTIONS = new HashMap<String, String>();

    static {
      USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
      USER_KERBEROS_OPTIONS.put("renewTGT", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
      }
    }

    private static final AppConfigurationEntry USER_KERBEROS_LOGIN =
      new AppConfigurationEntry(Krb5LoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                                USER_KERBEROS_OPTIONS);

    private static final AppConfigurationEntry[] USER_KERBEROS_CONF =
      new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN};

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      return USER_KERBEROS_CONF;
    }
  }

  private URL url;
  private HttpURLConnection conn;
  private Base64 base64;

  /**
   * Performs SPNEGO authentication against the specified URL.
   * <p/>
   * If a token is given it does a NOP and returns the given token.
   * <p/>
   * If no token is given, it will perform the SPNEGO authentication sequence using an
   * HTTP <code>OPTIONS</code> request.
   *
   * @param url the URl to authenticate against.
   * @param token the authentication token being used for the user.
   *
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication error occurred.
   */
  @Override
  public void authenticate(URL url, AuthenticatedURL.Token token)
    throws IOException, AuthenticationException {
    if (!token.isSet()) {
      this.url = url;
      base64 = new Base64(0);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(AUTH_HTTP_METHOD);
      conn.connect();
      if (isNegotiate()) {
        doSpnegoSequence(token);
      } else {
        getFallBackAuthenticator().authenticate(url, token);
      }
    }
  }

  /**
   * If the specified URL does not support SPNEGO authentication, a fallback {@link Authenticator} will be used.
   * <p/>
   * This implementation returns a {@link PseudoAuthenticator}.
   *
   * @return the fallback {@link Authenticator}.
   */
  protected Authenticator getFallBackAuthenticator() {
    return new PseudoAuthenticator();
  }

  /*
  * Indicates if the response is starting a SPNEGO negotiation.
  */
  private boolean isNegotiate() throws IOException {
    boolean negotiate = false;
    if (conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
      String authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
      negotiate = authHeader != null && authHeader.trim().startsWith(NEGOTIATE);
    }
    return negotiate;
  }

  /**
   * Implements the SPNEGO authentication sequence interaction using the current default principal
   * in the Kerberos cache (normally set via kinit).
   *
   * @param token the authentication token being used for the user.
   *
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication error occurred.
   */
  private void doSpnegoSequence(AuthenticatedURL.Token token) throws IOException, AuthenticationException {
    try {
      AccessControlContext context = AccessController.getContext();
      Subject subject = Subject.getSubject(context);
      if (subject == null) {
        subject = new Subject();
        LoginContext login = new LoginContext("", subject,
            null, new KerberosConfiguration());
        login.login();
      }
      Subject.doAs(subject, new PrivilegedExceptionAction<Void>() {

        @Override
        public Void run() throws Exception {
          GSSContext gssContext = null;
          try {
            GSSManager gssManager = GSSManager.getInstance();
            String servicePrincipal = "HTTP/" + KerberosAuthenticator.this.url.getHost();
            GSSName serviceName = gssManager.createName(servicePrincipal,
                                                        GSSUtil.NT_GSS_KRB5_PRINCIPAL);
            gssContext = gssManager.createContext(serviceName, GSSUtil.GSS_KRB5_MECH_OID, null,
                                                  GSSContext.DEFAULT_LIFETIME);
            gssContext.requestCredDeleg(true);
            gssContext.requestMutualAuth(true);

            byte[] inToken = new byte[0];
            byte[] outToken;
            boolean established = false;

            // Loop while the context is still not established
            while (!established) {
              outToken = gssContext.initSecContext(inToken, 0, inToken.length);
              if (outToken != null) {
                sendToken(outToken);
              }

              if (!gssContext.isEstablished()) {
                inToken = readToken();
              } else {
                established = true;
              }
            }
          } finally {
            if (gssContext != null) {
              gssContext.dispose();
              gssContext = null;
            }
          }
          return null;
        }
      });
    } catch (PrivilegedActionException ex) {
      throw new AuthenticationException(ex.getException());
    } catch (LoginException ex) {
      throw new AuthenticationException(ex);
    }
    AuthenticatedURL.extractToken(conn, token);
  }

  /*
  * Sends the Kerberos token to the server.
  */
  private void sendToken(byte[] outToken) throws IOException, AuthenticationException {
    String token = base64.encodeToString(outToken);
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(AUTH_HTTP_METHOD);
    conn.setRequestProperty(AUTHORIZATION, NEGOTIATE + " " + token);
    conn.connect();
  }

  /*
  * Retrieves the Kerberos token returned by the server.
  */
  private byte[] readToken() throws IOException, AuthenticationException {
    int status = conn.getResponseCode();
    if (status == HttpURLConnection.HTTP_OK || status == HttpURLConnection.HTTP_UNAUTHORIZED) {
      String authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
      if (authHeader == null || !authHeader.trim().startsWith(NEGOTIATE)) {
        throw new AuthenticationException("Invalid SPNEGO sequence, '" + WWW_AUTHENTICATE +
                                          "' header incorrect: " + authHeader);
      }
      String negotiation = authHeader.trim().substring((NEGOTIATE + " ").length()).trim();
      return base64.decode(negotiation);
    }
    throw new AuthenticationException("Invalid SPNEGO sequence, status code: " + status);
  }

}
