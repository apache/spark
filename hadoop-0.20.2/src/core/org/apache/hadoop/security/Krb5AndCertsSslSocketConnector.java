/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.Principal;
import java.util.List;
import java.util.Collections;
import java.util.Random;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.io.EndPoint;
import org.mortbay.jetty.HttpSchemes;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.security.ServletSSL;
import org.mortbay.jetty.security.SslSocketConnector;

/**
 * Extend Jetty's {@link SslSocketConnector} to optionally also provide 
 * Kerberos5ized SSL sockets.  The only change in behavior from superclass
 * is that we no longer honor requests to turn off NeedAuthentication when
 * running with Kerberos support.
 */
public class Krb5AndCertsSslSocketConnector extends SslSocketConnector {
  public static final List<String> KRB5_CIPHER_SUITES = 
    Collections.unmodifiableList(Collections.singletonList(
          "TLS_KRB5_WITH_3DES_EDE_CBC_SHA"));
  static {
    System.setProperty("https.cipherSuites", KRB5_CIPHER_SUITES.get(0));
  }
  
  private static final Log LOG = LogFactory
      .getLog(Krb5AndCertsSslSocketConnector.class);

  private static final String REMOTE_PRINCIPAL = "remote_principal";

  public enum MODE {KRB, CERTS, BOTH} // Support Kerberos, certificates or both?

  private final boolean useKrb;
  private final boolean useCerts;

  public Krb5AndCertsSslSocketConnector() {
    super();
    useKrb = true;
    useCerts = false;
    
    setPasswords();
  }
  
  public Krb5AndCertsSslSocketConnector(MODE mode) {
    super();
    useKrb = mode == MODE.KRB || mode == MODE.BOTH;
    useCerts = mode == MODE.CERTS || mode == MODE.BOTH;
    setPasswords();
    logIfDebug("useKerb = " + useKrb + ", useCerts = " + useCerts);
  }

  // If not using Certs, set passwords to random gibberish or else
  // Jetty will actually prompt the user for some.
  private void setPasswords() {
   if(!useCerts) {
     Random r = new Random();
     System.setProperty("jetty.ssl.password", String.valueOf(r.nextLong()));
     System.setProperty("jetty.ssl.keypassword", String.valueOf(r.nextLong()));
   }
  }
  
  @Override
  protected SSLServerSocketFactory createFactory() throws Exception {
    if(useCerts)
      return super.createFactory();
    
    SSLContext context = super.getProvider()==null
       ? SSLContext.getInstance(super.getProtocol())
        :SSLContext.getInstance(super.getProtocol(), super.getProvider());
    context.init(null, null, null);
    
    return context.getServerSocketFactory();
  }
  
  /* (non-Javadoc)
   * @see org.mortbay.jetty.security.SslSocketConnector#newServerSocket(java.lang.String, int, int)
   */
  @Override
  protected ServerSocket newServerSocket(String host, int port, int backlog)
      throws IOException {
    logIfDebug("Creating new KrbServerSocket for: " + host);
    SSLServerSocket ss = null;
    
    if(useCerts) // Get the server socket from the SSL super impl
      ss = (SSLServerSocket)super.newServerSocket(host, port, backlog);
    else { // Create a default server socket
      try {
        ss = (SSLServerSocket)(host == null 
         ? createFactory().createServerSocket(port, backlog) :
           createFactory().createServerSocket(port, backlog, InetAddress.getByName(host)));
      } catch (Exception e)
      {
        LOG.warn("Could not create KRB5 Listener", e);
        throw new IOException("Could not create KRB5 Listener: " + e.toString());
      }
    }
    
    // Add Kerberos ciphers to this socket server if needed.
    if(useKrb) {
      ss.setNeedClientAuth(true);
      String [] combined;
      if(useCerts) { // combine the cipher suites
        String[] certs = ss.getEnabledCipherSuites();
        combined = new String[certs.length + KRB5_CIPHER_SUITES.size()];
        System.arraycopy(certs, 0, combined, 0, certs.length);
        System.arraycopy(KRB5_CIPHER_SUITES.toArray(new String[0]), 0, combined,
              certs.length, KRB5_CIPHER_SUITES.size());
      } else { // Just enable Kerberos auth
        combined = KRB5_CIPHER_SUITES.toArray(new String[0]);
      }
      
      ss.setEnabledCipherSuites(combined);
    }
    
    return ss;
  };

  @Override
  public void customize(EndPoint endpoint, Request request) throws IOException {
    if(useKrb) { // Add Kerberos-specific info
      SSLSocket sslSocket = (SSLSocket)endpoint.getTransport();
      Principal remotePrincipal = sslSocket.getSession().getPeerPrincipal();
      logIfDebug("Remote principal = " + remotePrincipal);
      request.setScheme(HttpSchemes.HTTPS);
      request.setAttribute(REMOTE_PRINCIPAL, remotePrincipal);
      
      if(!useCerts) { // Add extra info that would have been added by super
        String cipherSuite = sslSocket.getSession().getCipherSuite();
        Integer keySize = Integer.valueOf(ServletSSL.deduceKeyLength(cipherSuite));;
        
        request.setAttribute("javax.servlet.request.cipher_suite", cipherSuite);
        request.setAttribute("javax.servlet.request.key_size", keySize);
      } 
    }
    
    if(useCerts) super.customize(endpoint, request);
  }
  
  private void logIfDebug(String s) {
    if(LOG.isDebugEnabled())
      LOG.debug(s);
  }
  
  /**
   * Filter that takes the Kerberos principal identified in the 
   * {@link Krb5AndCertsSslSocketConnector} and provides it the to the servlet
   * at runtime, setting the principal and short name.
   */
  public static class Krb5SslFilter implements Filter {
    @Override
    public void doFilter(ServletRequest req, ServletResponse resp,
        FilterChain chain) throws IOException, ServletException {
      final Principal princ = 
        (Principal)req.getAttribute(Krb5AndCertsSslSocketConnector.REMOTE_PRINCIPAL);
      
      if(princ == null || !(princ instanceof KerberosPrincipal)) {
        // Should never actually get here, since should be rejected at socket
        // level.
        LOG.warn("User not authenticated via kerberos from " + req.getRemoteAddr());
        ((HttpServletResponse)resp).sendError(HttpServletResponse.SC_FORBIDDEN, 
            "User not authenticated via Kerberos");
        return;
      }
      
      // Provide principal information for servlet at runtime
      ServletRequest wrapper = 
            new HttpServletRequestWrapper((HttpServletRequest) req) {
        @Override
        public Principal getUserPrincipal() {
          return princ;
        }
        
        /* 
         * Return the full name of this remote user.
         * @see javax.servlet.http.HttpServletRequestWrapper#getRemoteUser()
         */
        @Override 
        public String getRemoteUser() {
          return princ.getName();
        }
      };
      
      chain.doFilter(wrapper, resp);
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
      /* Nothing to do here */
    }

    @Override
    public void destroy() { /* Nothing to do here */ }
  }
}
