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
package org.apache.spark.network.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.google.common.io.Files;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.network.util.JavaUtils;

public class SSLFactory {
  private static final SparkLogger logger = SparkLoggerFactory.getLogger(SSLFactory.class);

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

  private void initJdkSslContext(final Builder b)
          throws IOException, GeneralSecurityException {
    this.keyManagers = keyManagers(b.keyStore, b.keyPassword, b.keyStorePassword);
    this.trustManagers = trustStoreManagers(
      b.trustStore, b.trustStorePassword,
      b.trustStoreReloadingEnabled, b.trustStoreReloadIntervalMs
    );
    this.jdkSslContext = createSSLContext(requestedProtocol, keyManagers, trustManagers);
  }

  private void initNettySslContexts(final Builder b)
          throws SSLException {
    nettyClientSslContext = SslContextBuilder
      .forClient()
      .sslProvider(getSslProvider(b))
      .trustManager(b.certChain)
      .build();

    nettyServerSslContext = SslContextBuilder
      .forServer(b.certChain, b.privateKey, b.privateKeyPassword)
      .sslProvider(getSslProvider(b))
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
        logger.warn("OpenSSL Provider requested but it is not available, using JDK SSL Provider");
      }
    }
    return SslProvider.JDK;
  }

  public void destroy() {
    if (trustManagers != null) {
      for (int i = 0; i < trustManagers.length; i++) {
        if (trustManagers[i] instanceof ReloadingX509TrustManager manager) {
          try {
            manager.destroy();
          } catch (InterruptedException ex) {
            logger.info("Interrupted while destroying trust manager: ", ex);
          }
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
    private String privateKeyPassword;
    private String keyPassword;
    private File certChain;
    private File trustStore;
    private String trustStorePassword;
    private boolean trustStoreReloadingEnabled;
    private int trustStoreReloadIntervalMs;
    private boolean openSslEnabled;

    /**
     * Sets the requested protocol, i.e., "TLSv1.2", "TLSv1.1", etc
     *
     * @param requestedProtocol The requested protocol
     * @return The builder object
     */
    public Builder requestedProtocol(String requestedProtocol) {
      this.requestedProtocol = requestedProtocol == null ? "TLSv1.3" : requestedProtocol;
      return this;
    }

    /**
     * Sets the requested cipher suites, i.e., "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", etc
     *
     * @param requestedCiphers The requested ciphers
     * @return The builder object
     */
    public Builder requestedCiphers(String[] requestedCiphers) {
      this.requestedCiphers = requestedCiphers;
      return this;
    }

    /**
     * Sets the Keystore and Keystore password
     *
     * @param keyStore The key store file to use
     * @param keyStorePassword The password for the key store
     * @return The builder object
     */
    public Builder keyStore(File keyStore, String keyStorePassword) {
      this.keyStore = keyStore;
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    /**
     * Sets a PKCS#8 private key file in PEM format
     *
     * @param privateKey The private key file to use
     * @return The builder object
     */
    public Builder privateKey(File privateKey) {
      this.privateKey = privateKey;
      return this;
    }

    /**
     * Sets the key password
     *
     * @param keyPassword The password for the private key in the key store
     * @return The builder object
     */
    public Builder keyPassword(String keyPassword) {
      this.keyPassword = keyPassword;
      return this;
    }

    /**
     * Sets the private key password
     *
     * @param privateKeyPassword The password for the private key
     * @return The builder object
     */
    public Builder privateKeyPassword(String privateKeyPassword) {
      this.privateKeyPassword = privateKeyPassword;
      return this;
    }

    /**
     * Sets a X.509 certificate chain file in PEM format
     *
     * @param certChain The certificate chain file to use
     * @return The builder object
     */
    public Builder certChain(File certChain) {
      this.certChain = certChain;
      return this;
    }

    /**
     * @param enabled Whether to use the OpenSSL implementation
     * @return The builder object
     */
    public Builder openSslEnabled(boolean enabled) {
      this.openSslEnabled = enabled;
      return this;
    }

    /**
     * Sets the trust-store, trust-store password, whether to use a Reloading TrustStore,
     * and the trust-store reload interval, if enabled
     *
     * @param trustStore The trust store file to use
     * @param trustStorePassword The password for the trust store
     * @param trustStoreReloadingEnabled Whether trust store reloading is enabled
     * @param trustStoreReloadIntervalMs The interval at which to reload the trust store file
     * @return The builder object
     */
    public Builder trustStore(
        File trustStore, String trustStorePassword,
        boolean trustStoreReloadingEnabled, int trustStoreReloadIntervalMs) {
      this.trustStore = trustStore;
      this.trustStorePassword = trustStorePassword;
      this.trustStoreReloadingEnabled = trustStoreReloadingEnabled;
      this.trustStoreReloadIntervalMs = trustStoreReloadIntervalMs;
      return this;
    }

    /**
     * Builds our {@link SSLFactory}
     *
     * @return The built {@link SSLFactory}
     */
    public SSLFactory build() {
      return new SSLFactory(this);
    }
  }

  /**
   * Returns an initialized {@link SSLContext}
   *
   * @param requestedProtocol The requested protocol to use
   * @param keyManagers The list of key managers to use
   * @param trustManagers The list of trust managers to use
   * @return The built {@link SSLContext}
   * @throws GeneralSecurityException
   */
  private static SSLContext createSSLContext(
      String requestedProtocol,
      KeyManager[] keyManagers,
      TrustManager[] trustManagers) throws GeneralSecurityException {
    SSLContext sslContext = SSLContext.getInstance(requestedProtocol);
    sslContext.init(keyManagers, trustManagers, null);
    return sslContext;
  }

  /**
   * Creates a new {@link SSLEngine}.
   * Note that currently client auth is not supported
   *
   * @param isClient Whether the engine is used in a client context
   * @param allocator The {@link ByteBufAllocator to use}
   * @return A valid {@link SSLEngine}.
   */
  public SSLEngine createSSLEngine(boolean isClient, ByteBufAllocator allocator) {
    SSLEngine engine = createEngine(isClient, allocator);
    engine.setUseClientMode(isClient);
    engine.setNeedClientAuth(false);
    engine.setEnabledProtocols(enabledProtocols(engine, requestedProtocol));
    engine.setEnabledCipherSuites(enabledCipherSuites(engine, requestedCiphers));
    return engine;
  }

  private SSLEngine createEngine(boolean isClient, ByteBufAllocator allocator) {
    SSLEngine engine;
    if (isClient) {
      if (nettyClientSslContext != null) {
        engine = nettyClientSslContext.newEngine(allocator);
      } else {
        engine = jdkSslContext.createSSLEngine();
      }
    } else {
      if (nettyServerSslContext != null) {
        engine = nettyServerSslContext.newEngine(allocator);
      } else {
        engine = jdkSslContext.createSSLEngine();
      }
    }
    return engine;
  }

  private static TrustManager[] credulousTrustStoreManagers() {
    return new TrustManager[]{new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
        throws CertificateException {
      }

      @Override
      public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
        throws CertificateException {
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return null;
      }
    }};
  }

  private static TrustManager[] trustStoreManagers(
      File trustStore, String trustStorePassword,
      boolean trustStoreReloadingEnabled, int trustStoreReloadIntervalMs)
          throws IOException, GeneralSecurityException {
    if (trustStore == null || !trustStore.exists()) {
      return credulousTrustStoreManagers();
    } else {
      if (trustStoreReloadingEnabled) {
        ReloadingX509TrustManager reloading = new ReloadingX509TrustManager(
          KeyStore.getDefaultType(), trustStore, trustStorePassword, trustStoreReloadIntervalMs);
        reloading.init();
        return new TrustManager[]{reloading};
      } else {
        return defaultTrustManagers(trustStore, trustStorePassword);
      }
    }
  }

  private static TrustManager[] defaultTrustManagers(File trustStore, String trustStorePassword)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    try (InputStream input = Files.asByteSource(trustStore).openStream()) {
      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      char[] passwordCharacters = trustStorePassword != null?
        trustStorePassword.toCharArray() : null;
      ks.load(input, passwordCharacters);
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      return tmf.getTrustManagers();
    }
  }

  private static KeyManager[] keyManagers(
    File keyStore, String keyPassword, String keyStorePassword)
      throws NoSuchAlgorithmException, CertificateException,
          KeyStoreException, IOException, UnrecoverableKeyException {
    KeyManagerFactory factory = KeyManagerFactory.getInstance(
      KeyManagerFactory.getDefaultAlgorithm());
    char[] keyStorePasswordChars = keyStorePassword != null? keyStorePassword.toCharArray() : null;
    char[] keyPasswordChars = keyPassword != null?
      keyPassword.toCharArray() : keyStorePasswordChars;
    factory.init(loadKeyStore(keyStore, keyStorePasswordChars), keyPasswordChars);
    return factory.getKeyManagers();
  }

  private static KeyStore loadKeyStore(File keyStore, char[] keyStorePassword)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
    if (keyStore == null) {
      throw new KeyStoreException(
        "keyStore cannot be null. Please configure spark.ssl.rpc.keyStore");
    }

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    FileInputStream fin = new FileInputStream(keyStore);
    try {
      ks.load(fin, keyStorePassword);
      return ks;
    } finally {
      JavaUtils.closeQuietly(fin);
    }
  }

  private static String[] enabledProtocols(SSLEngine engine, String requestedProtocol) {
    String[] supportedProtocols = engine.getSupportedProtocols();
    String[] defaultProtocols = {"TLSv1.3", "TLSv1.2"};
    String[] enabledProtocols =
      ((requestedProtocol == null || requestedProtocol.isEmpty()) ?
        defaultProtocols : new String[]{requestedProtocol});

    List<String> protocols = addIfSupported(supportedProtocols, enabledProtocols);
    if (!protocols.isEmpty()) {
      return protocols.toArray(new String[protocols.size()]);
    } else {
      return supportedProtocols;
    }
  }

  private static String[] enabledCipherSuites(
      String[] supportedCiphers, String[] defaultCiphers, String[] requestedCiphers) {
    String[] baseCiphers = new String[]{
      // We take ciphers from the mozilla modern list first (for TLS 1.3):
      // https://wiki.mozilla.org/Security/Server_Side_TLS
      "TLS_CHACHA20_POLY1305_SHA256",
      "TLS_AES_128_GCM_SHA256",
      "TLS_AES_256_GCM_SHA384",
      // Next we have the TLS1.2 ciphers for intermediate compatibility (since JDK8 does not
      // support TLS1.3)
      "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
      "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
      "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"
    };
    String[] enabledCiphers =
      ((requestedCiphers == null || requestedCiphers.length == 0) ? baseCiphers : requestedCiphers);

    List<String> ciphers = addIfSupported(supportedCiphers, enabledCiphers);
    if (!ciphers.isEmpty()) {
      return ciphers.toArray(new String[ciphers.size()]);
    } else {
      // Use the default from JDK as fallback.
      return defaultCiphers;
    }
  }

  private static String[] enabledCipherSuites(SSLEngine engine, String[] requestedCiphers) {
    return enabledCipherSuites(
      engine.getSupportedCipherSuites(), engine.getEnabledCipherSuites(), requestedCiphers);
  }

  private static List<String> addIfSupported(String[] supported, String... names) {
    List<String> enabled = new ArrayList<>();
    Set<String> supportedSet = new HashSet<>(Arrays.asList(supported));
    for (String n : names) {
      if (supportedSet.contains(n)) {
        enabled.add(n);
      }
    }
    return enabled;
  }
}
