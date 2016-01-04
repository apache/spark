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

import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.bouncycastle.x509.X509V1CertificateGenerator;

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;

/**
 *
 */
public class SslSampleConfigs {

  public static final String keyStorePath = getAbsolutePath("/keystore");
  public static final String privateKeyPath = getAbsolutePath("/key.pem");
  public static final String certChainPath = getAbsolutePath("/certchain.pem");
  public static final String untrustedKeyStorePath = getAbsolutePath("/untrusted-keystore");
  public static final String trustStorePath = getAbsolutePath("/truststore");

  /**
   * @return
   */
  public static ConfigProvider createDefaultConfigProvider() {
    return new SystemPropertyConfigProvider() {
      @Override
      public boolean getBoolean(String name, boolean defaultValue) {
        if (name.equals("spark.ssl.bts.enabled")) {
          return true;
        } else if (name.equals("spark.ssl.bts.trustStoreReloadingEnabled")) {
          return false;
        } else if (name.equals("spark.ssl.bts.openSslEnabled")) {
          return false;
        } else {
          return super.getBoolean(name, defaultValue);
        }
      }

      @Override
      public int getInt(String name, int defaultValue) {
        if (name.equals("spark.ssl.bts.trustStoreReloadInterval")) {
          return 10000;
        } else {
          return super.getInt(name, defaultValue);
        }
      }

      @Override
      public String get(String name, String defaultValue) {
        if (name.equals("spark.ssl.bts.keyStore")) {
          return SslSampleConfigs.keyStorePath;
        } else if (name.equals("spark.ssl.bts.keyStorePassword")) {
          return "password";
        } else if (name.equals("spark.ssl.bts.privateKey")) {
          return SslSampleConfigs.privateKeyPath;
        } else if (name.equals("spark.ssl.bts.keyPassword")) {
          return "password";
        } else if (name.equals("spark.ssl.bts.certChain")) {
          return SslSampleConfigs.certChainPath;
        } else if (name.equals("spark.ssl.bts.trustStore")) {
          return SslSampleConfigs.trustStorePath;
        } else if (name.equals("spark.ssl.bts.trustStorePassword")) {
          return "password";
        } else if (name.equals("spark.ssl.bts.enabledAlgorithms")) {
            return "TLS_RSA_WITH_AES_128_CBC_SHA";
        } else {
          return super.get(name, defaultValue);
        }
      }
    };
  }

  /**
   * Create a new {@link SSLFactory} with a trusted keystore.
   *
   * @return
   * @throws CertificateException
   * @throws UnrecoverableKeyException
   * @throws NoSuchAlgorithmException
   * @throws KeyStoreException
   * @throws KeyManagementException
   * @throws IOException
   */
  public static SSLFactory createTrustedSSLFactory() {
    try {
      return new SSLFactory.Builder()
        .requestedProtocol("TLSv1")
        .requestedCiphers(new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA"})
        .keyStore(new File(keyStorePath), "password")
        .keyPassword("password")
        .trustStore(
          new File(trustStorePath),
          "password",
          false,
          10000)
        .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create trusted SSLEngine", e);
    }
  }

  /**
   * Create a new Netty based {@link SSLFactory} with a X509 cert chain file in PEM format,
   * and a PKCS#8 private key file in PEM format.
   *
   * @return
   */
  public static SSLFactory createNettySSLFactory() {
    try {
      return new SSLFactory.Builder()
        .requestedProtocol("TLSv1") //spark.ssl.protocol
        .privateKey(new File(privateKeyPath)) //spark.ssl.keyFile
        .keyPassword("password") //spark.ssl.keyPassword
        .certChain(new File(certChainPath)) //spark.ssl.certChain
        .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create trusted SSLEngine", e);
    }
  }

  /**
   * Create a new Netty based {@link SSLFactory} with a X509 cert chain file in PEM format,
   * and a PKCS#8 private key file in PEM format.
   *
   * @return
   */
  public static SSLFactory createOpenSSLNettySSLFactory(boolean isClient) {
    try {
      return new SSLFactory.Builder()
        .requestedProtocol("TLSv1")
        .requestedCiphers(new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA"})
        .privateKey(new File(privateKeyPath))
        .keyPassword("password")
        .certChain(new File(certChainPath))
        .openSslEnabled(true)
        .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create trusted SSLEngine", e);
    }
  }

  /**
   * Create a new {@link SSLFactory} with an untrusted keystore.
   *
   * @return
   * @throws CertificateException
   * @throws UnrecoverableKeyException
   * @throws NoSuchAlgorithmException
   * @throws KeyStoreException
   * @throws KeyManagementException
   * @throws IOException
   */
  public static SSLFactory createUntrustedSSLFactory(boolean isClient) {
    try {
      return new SSLFactory.Builder()
        .requestedProtocol("TLSv1")
        .requestedCiphers(new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA"})
        .keyStore(new File(untrustedKeyStorePath), "password")
        .keyPassword("password")
        .trustStore(
          new File(trustStorePath),
          "password",
          false,
          10000)
        .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create untrusted SSLEngine", e);
    }
  }

  /**
   * @param trustStore
   * @param password
   * @param alias
   * @param cert
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static void createTrustStore(
    File trustStore, String password, String alias, Certificate cert)
    throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setCertificateEntry(alias, cert);
    saveKeyStore(ks, trustStore, password);
  }

  /**
   * Creates a keystore with multiple keys and saves it to a file.
   *
   * @param trustStore
   * @param password
   * @param certs
   * @param <T>
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static <T extends Certificate> void createTrustStore(
    File trustStore, String password, Map<String, T> certs)
    throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, trustStore, password);
  }

  /**
   * Create a self-signed X.509 Certificate.
   *
   * @param dn        the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair      the KeyPair
   * @param days      how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   */
  @SuppressWarnings("deprecation")
  public static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm)
    throws CertificateEncodingException,
    InvalidKeyException,
    IllegalStateException,
    NoSuchProviderException, NoSuchAlgorithmException, SignatureException {

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000l);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    KeyPair keyPair = pair;
    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
    X500Principal dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm(algorithm);

    X509Certificate cert = certGen.generate(pair.getPrivate());
    return cert;
  }

  /**
   * @param algorithm
   * @return
   * @throws NoSuchAlgorithmException
   */
  public static KeyPair generateKeyPair(String algorithm)
    throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  /**
   * Creates a keystore with a single key and saves it to a file.
   *
   * @param keyStore    File keystore to save
   * @param password    String store password to set on keystore
   * @param keyPassword String key password to set on key
   * @param alias       String alias to use for the key
   * @param privateKey  Key to save in keystore
   * @param cert        Certificate to use as certificate chain associated to key
   * @throws GeneralSecurityException for any error with the security APIs
   * @throws IOException              if there is an I/O error saving the file
   */
  public static void createKeyStore(
    File keyStore, String password, String keyPassword,
    String alias, Key privateKey, Certificate cert)
    throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray(),
      new Certificate[]{cert});
    saveKeyStore(ks, keyStore, password);
  }

  /**
   * @param keyStore
   * @param password
   * @param alias
   * @param privateKey
   * @param cert
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static void createKeyStore(
    File keyStore, String password,
    String alias, Key privateKey, Certificate cert)
    throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(), new Certificate[]{cert});
    saveKeyStore(ks, keyStore, password);
  }

  private static KeyStore createEmptyKeyStore()
    throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(
    KeyStore ks, File keyStore, String password)
    throws GeneralSecurityException, IOException {
    FileOutputStream out = new FileOutputStream(keyStore);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }

  /**
   * @param path
   * @return
   */
  private static String getAbsolutePath(String path) {
    try {
      return new File(SslSampleConfigs.class.getResource(path).toURI()).getAbsolutePath();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to resolve path " + path, e);
    }
  }
}
