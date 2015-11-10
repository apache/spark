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
package org.apache.spark.network.util.ssl;

import com.google.common.io.Files;

import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.network.util.JavaUtils;

/**
 *
 */
public class SSLFactory {

  private final Logger logger = LoggerFactory.getLogger(SSLFactory.class);

  /**
   * For a configuration specifying keystore/truststore files
   */
  private SSLContext jdkSslContext;

  /**
   * For a configuration specifying a PEM cert chain, and a PEM private key
   */
  private SslContext nettyClientSslContext;
  private SslContext nettyServerSslContext;

  private KeyManager[] keyManagers;
  private TrustManager[] trustManagers;
  private String requestedProtocol;
  private String[] requestedCiphers;

  /**
   * @param b
   */
  private SSLFactory(final Builder b) {
    this.requestedProtocol = b.requestedProtocol;
    this.requestedCiphers = b.requestedCiphers;
    try {
      if (b.certChain != null && b.privateKey != null) {
        initNettySslContexts(b);
      } else {
        initJdkSslContext(b);
      }
    } catch (Exception e) {
      throw new RuntimeException("SSLFactory creation failed", e);
    }
  }

  /**
   * @param b
   * @throws IOException
   * @throws GeneralSecurityException
   */
  private void initJdkSslContext(final Builder b)
    throws IOException, GeneralSecurityException {
    this.keyManagers = keyManagers(b.keyStore, b.keyStorePassword);
    this.trustManagers = trustStoreManagers(
      b.trustStore, b.trustStorePassword,
      b.trustStoreReloadingEnabled, b.trustStoreReloadInterval);
    this.jdkSslContext = createSSLContext(requestedProtocol, keyManagers, trustManagers);
  }

  /**
   * @param b
   * @throws SSLException
   */
  private void initNettySslContexts(final Builder b)
    throws SSLException, NoSuchAlgorithmException {
    nettyClientSslContext = SslContextBuilder
      .forClient()
      .trustManager(b.certChain)
      .build();

    nettyServerSslContext = SslContextBuilder
      .forServer(b.certChain, b.privateKey, b.keyPassword)
      .sslProvider(getSslProvider(b))
      .ciphers(Arrays.asList(enabledCipherSuites(b.requestedCiphers, b.requestedProtocol)))
      .sessionCacheSize(0)
      .sessionTimeout(0)
      .build();
  }

  /**
   * If OpenSSL is requested, this will check if an implementation is available on the local host.
   * If an implementation is not available it will fall back to the JDK {@link SslProvider}.
   *
   * @param b
   * @return
   */
  private SslProvider getSslProvider(Builder b) {
    if (b.openSslEnabled) {
      if (OpenSsl.isAvailable()) {
        return SslProvider.OPENSSL;
      } else {
        logger.info("OpenSSL Provider requested but implementation is not available, using JDK SSL Provider");
      }
    }
    return SslProvider.JDK;
  }

  /**
   *
   */
  public void destroy() {
    if (trustManagers != null) {
      for (int i = 0; i < trustManagers.length; i++) {
        if (trustManagers[i] instanceof ReloadingX509TrustManager) {
          ((ReloadingX509TrustManager) trustManagers[i]).destroy();
        }
      }
      trustManagers = null;
    }

    keyManagers = null;
    jdkSslContext = null;
    nettyClientSslContext = null;
    nettyServerSslContext = null;
    requestedProtocol = null;
    requestedCiphers = null;
  }

  /**
   * Builder class to construct instances of {@link SSLFactory} with specific options
   */
  public static class Builder {
    private String requestedProtocol;
    private String[] requestedCiphers;
    private File keyStore;
    private String keyStorePassword;
    private File privateKey;
    private String keyPassword;
    private File certChain;
    private File trustStore;
    private String trustStorePassword;
    private boolean trustStoreReloadingEnabled;
    private int trustStoreReloadInterval;
    private boolean openSslEnabled;

    /**
     * Sets the requested protocol, i.e., "TLSv1.2", "TLSv1.1", etc
     *
     * @param requestedProtocol
     * @return
     */
    public Builder requestedProtocol(String requestedProtocol) {
      this.requestedProtocol = requestedProtocol == null ? "TLSv1.2" : requestedProtocol;
      return this;
    }

    /**
     * Sets the requested cipher suites, i.e., "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", etc
     *
     * @param requestedCiphers
     * @return
     */
    public Builder requestedCiphers(String[] requestedCiphers) {
      this.requestedCiphers = requestedCiphers;
      return this;
    }

    /**
     * Sets the Keystore and Keystore password
     *
     * @param keyStore
     * @param keyStorePassword
     * @return
     */
    public Builder keyStore(File keyStore, String keyStorePassword) {
      this.keyStore = keyStore;
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    /**
     * Sets a PKCS#8 private key file in PEM format
     *
     * @param privateKey
     * @return
     */
    public Builder privateKey(File privateKey) {
      this.privateKey = privateKey;
      return this;
    }

    /**
     * Sets the Key password
     *
     * @param keyPassword
     * @return
     */
    public Builder keyPassword(String keyPassword) {
      this.keyPassword = keyPassword;
      return this;
    }

    /**
     * Sets a X.509 certificate chain file in PEM format
     *
     * @param certChain
     * @return
     */
    public Builder certChain(File certChain) {
      this.certChain = certChain;
      return this;
    }

    /**
     * @param enabled
     * @return
     */
    public Builder openSslEnabled(boolean enabled) {
      this.openSslEnabled = enabled;
      return this;
    }

    /**
     * Sets the trust-store, trust-store password, whether to use a Reloading TrustStore,
     * and the trust-store reload interval, if enabled
     *
     * @param trustStore
     * @param trustStorePassword
     * @param trustStoreReloadingEnabled
     * @param trustStoreReloadInterval
     * @return
     */
    public Builder trustStore(
      File trustStore, String trustStorePassword,
      boolean trustStoreReloadingEnabled, int trustStoreReloadInterval) {
      this.trustStore = trustStore;
      this.trustStorePassword = trustStorePassword;
      this.trustStoreReloadingEnabled = trustStoreReloadingEnabled;
      this.trustStoreReloadInterval = trustStoreReloadInterval;
      return this;
    }

    /**
     * Builds our {@link SSLFactory}
     *
     * @return
     */
    public SSLFactory build() {
      return new SSLFactory(this);
    }
  }

  /**
   * Returns an initialized {@link SSLContext}
   *
   * @param requestedProtocol
   * @param keyManagers
   * @param trustManagers
   * @return
   * @throws IOException
   * @throws GeneralSecurityException
   */
  private static SSLContext createSSLContext(
    String requestedProtocol,
    KeyManager[] keyManagers,
    TrustManager[] trustManagers)
    throws IOException, GeneralSecurityException {
    SSLContext sslContext = getSSLContextInstance(requestedProtocol);
    sslContext.init(keyManagers, trustManagers, null);
    return sslContext;
  }

  /**
   * Get the {@link SSLContext} for the specified <tt>requestedProtocol</tt>
   * if available, or the default {@linnk SSLContext}
   * @param requestedProtocol
   * @return
   * @throws NoSuchAlgorithmException
   */
  private static SSLContext getSSLContextInstance(String requestedProtocol)
    throws NoSuchAlgorithmException {
    SSLContext context = null;
    try {
      context = SSLContext.getInstance(requestedProtocol);
    } catch (Exception e) {
      context = SSLContext.getDefault();
    }
    return context;
  }

  /**
   * Creates a new {@link SSLEngine}.
   * Note that currently client auth is not supported
   *
   * @return
   * @throws NoSuchAlgorithmException
   * @throws UnrecoverableKeyException
   * @throws CertificateException
   * @throws KeyStoreException
   * @throws IOException
   * @throws KeyManagementException
   */
  public SSLEngine createSSLEngine(boolean isClient)
    throws NoSuchAlgorithmException, UnrecoverableKeyException,
    CertificateException, KeyStoreException, IOException, KeyManagementException {
    SSLEngine engine = createEngine(isClient);
    engine.setUseClientMode(isClient);
    engine.setNeedClientAuth(false);
    engine.setEnabledProtocols(enabledProtocols(engine, requestedProtocol));
    engine.setEnabledCipherSuites(enabledCipherSuites(engine, requestedCiphers));
    return engine;
  }

  /**
   * @param isClient
   * @return
   */
  private SSLEngine createEngine(boolean isClient) {
    SSLEngine engine;
    if (isClient) {
      engine =
        (nettyClientSslContext != null ? nettyClientSslContext.newEngine(null) : jdkSslContext.createSSLEngine());
    } else {
      engine = (nettyServerSslContext != null ? nettyServerSslContext.newEngine(null) : jdkSslContext.createSSLEngine());
    }
    return engine;
  }

  /**
   * Trust All....
   *
   * @return
   */
  private static TrustManager[] credulousTrustStoreManagers() {
    return new TrustManager[]{new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      }

      @Override
      public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return null;
      }
    }};
  }

  /**
   * @param trustStore
   * @param trustStorePassword
   * @return
   * @throws IOException
   * @throws CertificateException
   * @throws NoSuchAlgorithmException
   * @throws KeyStoreException
   */
  private static TrustManager[] trustStoreManagers(
    File trustStore, String trustStorePassword,
    boolean trustStoreReloadingEnabled, int trustStoreReloadInterval)
    throws IOException, GeneralSecurityException {
    if (trustStore == null || !trustStore.exists()) {
      return credulousTrustStoreManagers();
    } else {
      if (trustStorePassword == null)
        throw new KeyStoreException("trustStorePassword cannot be null");

      if (trustStoreReloadingEnabled) {
        ReloadingX509TrustManager reloading = new ReloadingX509TrustManager(
          KeyStore.getDefaultType(), trustStore, trustStorePassword, trustStoreReloadInterval);
        reloading.init();
        return new TrustManager[]{reloading};
      } else {
        return defaultTrustManagers(trustStore, trustStorePassword);
      }
    }
  }

  /**
   * @param trustStore
   * @param trustStorePassword
   * @return
   * @throws IOException
   * @throws KeyStoreException
   * @throws CertificateException
   * @throws NoSuchAlgorithmException
   */
  private static TrustManager[] defaultTrustManagers(File trustStore, String trustStorePassword)
    throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    InputStream input = Files.asByteSource(trustStore).openStream();
    try {
      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(input, trustStorePassword.toCharArray());
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      return tmf.getTrustManagers();
    } finally {
      JavaUtils.closeQuietly(input);
    }
  }

  /**
   * @param keyStore
   * @param keyStorePassword
   * @return
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   * @throws KeyStoreException
   * @throws IOException
   * @throws UnrecoverableKeyException
   */
  private static KeyManager[] keyManagers(File keyStore, String keyStorePassword)
    throws NoSuchAlgorithmException, CertificateException,
    KeyStoreException, IOException, UnrecoverableKeyException {
    KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    factory.init(loadKeyStore(keyStore, keyStorePassword),
      (keyStorePassword != null ? keyStorePassword.toCharArray() : null));

    return factory.getKeyManagers();
  }

  /**
   * @param keyStore
   * @param keyStorePassword
   * @return
   * @throws KeyStoreException
   * @throws IOException
   * @throws CertificateException
   * @throws NoSuchAlgorithmException
   */
  private static KeyStore loadKeyStore(File keyStore, String keyStorePassword)
    throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
    if (keyStore == null)
      throw new KeyStoreException("keyStore cannot be null");

    if (keyStorePassword == null)
      throw new KeyStoreException("keyStorePassword cannot be null");

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    FileInputStream fin = new FileInputStream(keyStore);
    try {
      ks.load(fin, keyStorePassword.toCharArray());
      return ks;
    } finally {
      JavaUtils.closeQuietly(fin);
    }
  }

  /**
   * @param engine
   * @param requestedProtocol
   * @return
   */
  private static String[] enabledProtocols(SSLEngine engine, String requestedProtocol) {
    String[] supportedProtocols = engine.getSupportedProtocols();
    String[] defaultProtocols = {"TLSv1.2", "TLSv1.1", "TLSv1"};
    String[] enabledProtocols =
      ((requestedProtocol == null || requestedProtocol.isEmpty()) ? defaultProtocols : new String[]{requestedProtocol});
    List<String> protocols = new ArrayList<String>();

    addIfSupported(supportedProtocols, protocols, enabledProtocols);
    if (!protocols.isEmpty()) {
      return protocols.toArray(new String[protocols.size()]);
    } else {
      return supportedProtocols;
    }
  }

  /**
   *
   * @param supportedCiphers
   * @param defaultCiphers
   * @param requestedCiphers
   * @return
   */
  private static String[] enabledCipherSuites(
    String[] supportedCiphers, String[] defaultCiphers, String[] requestedCiphers) {
    String[] baseCiphers = new String[]{
      // GCM (Galois/Counter Mode) requires JDK 8.
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
      // AES256 requires JCE unlimited strength jurisdiction policy files.
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
      // GCM (Galois/Counter Mode) requires JDK 8.
      "TLS_RSA_WITH_AES_128_GCM_SHA256",
      "SSL_RSA_WITH_RC4_128_SHA",
      "SSL_RSA_WITH_RC4_128_MD5",
      "TLS_RSA_WITH_AES_128_CBC_SHA",
      // AES256 requires JCE unlimited strength jurisdiction policy files.
      "TLS_RSA_WITH_AES_256_CBC_SHA"};
    String[] enabledCiphers =
      ((requestedCiphers == null || requestedCiphers.length == 0) ? baseCiphers : requestedCiphers);
    List<String> ciphers = new ArrayList<String>();

    addIfSupported(supportedCiphers, ciphers, enabledCiphers);
    if (!ciphers.isEmpty()) {
      return ciphers.toArray(new String[ciphers.size()]);
    } else {
      // Use the default from JDK as fallback.
      return defaultCiphers;
    }
  }

  /**
   * @param engine
   * @param requestedCiphers
   * @return
   */
  private static String[] enabledCipherSuites(SSLEngine engine, String[] requestedCiphers) {
    return enabledCipherSuites(
      engine.getSupportedCipherSuites(), engine.getEnabledCipherSuites(), requestedCiphers);
  }

  /**
   *
   * @param requestedCiphers
   * @param requestedProtocol
   * @return
   * @throws NoSuchAlgorithmException
   */
  private static String[] enabledCipherSuites(String[] requestedCiphers, String requestedProtocol)
    throws NoSuchAlgorithmException {
    SSLContext context = null;
    try {
      context = SSLContext.getInstance(requestedProtocol);
      context.init(null, null, null);
    } catch (Exception e) {
      context = SSLContext.getDefault();
    }
    SSLServerSocketFactory factory = context.getServerSocketFactory();
    return enabledCipherSuites(
      factory.getSupportedCipherSuites(), factory.getDefaultCipherSuites(), requestedCiphers);
  }

  /**
   * @param supported
   * @param enabled
   * @param names
   */
  private static void addIfSupported(String[] supported, List<String> enabled, String... names) {
    for (String n : names) {
      for (String s : supported) {
        if (n.equals(s)) {
          enabled.add(s);
          break;
        }
      }
    }
  }
}
