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

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.*;

import org.bouncycastle.x509.X509V1CertificateGenerator;

import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.MapConfigProvider;


public class SslSampleConfigs {

  public static final String keyStorePath = getAbsolutePath("/keystore");
  public static final String privateKeyPath = getAbsolutePath("/key.pem");
  public static final String certChainPath = getAbsolutePath("/certchain.pem");
  public static final String untrustedKeyStorePath = getAbsolutePath("/untrusted-keystore");
  public static final String trustStorePath = getAbsolutePath("/truststore");
  public static final String unencryptedPrivateKeyPath = getAbsolutePath("/unencrypted-key.pem");
  public static final String unencryptedCertChainPath =
    getAbsolutePath("/unencrypted-certchain.pem");

  /**
   * Creates a config map containing the settings needed to enable the RPC SSL feature
   * All the settings (except the enabled one) are intentionally set on the parent namespace
   * so that we can verify settings inheritance works. We intentionally set conflicting
   * options for the key password to verify that is handled correctly.
   */
  public static Map<String, String> createDefaultConfigMap() {
    Map<String, String> confMap = new HashMap<String, String>();
    confMap.put("spark.ssl.rpc.enabled", "true");
    confMap.put("spark.ssl.rpc.openSslEnabled", "true");
    confMap.put("spark.ssl.rpc.privateKey", SslSampleConfigs.unencryptedPrivateKeyPath);
    // intentionally not set
    // confMap.put("spark.ssl.rpc.privateKeyPassword", "password");
    confMap.put("spark.ssl.rpc.certChain", SslSampleConfigs.unencryptedCertChainPath);
    confMap.put("spark.ssl.enabled", "true");
    confMap.put("spark.ssl.keyPassword", "password");
    confMap.put("spark.ssl.trustStoreReloadingEnabled", "false");
    confMap.put("spark.ssl.trustStoreReloadIntervalMs", "10000");
    confMap.put("spark.ssl.keyStore", SslSampleConfigs.keyStorePath);
    confMap.put("spark.ssl.keyStorePassword", "password");
    confMap.put("spark.ssl.trustStore", SslSampleConfigs.trustStorePath);
    confMap.put("spark.ssl.trustStorePassword", "password");
    confMap.put("spark.ssl.protocol", "TLSv1.3");
    confMap.put("spark.ssl.standalone.enabled", "true");
    confMap.put("spark.ssl.ui.enabled", "true");
    return confMap;
  }

  /**
   * Similar to the above, but sets the settings directly in the spark.ssl.rpc namespace
   * This is needed for testing in the lower level modules (like network-common) where inheritance
   * does not work as there is no access to SSLOptions.
   */
  public static Map<String, String> createDefaultConfigMapForRpcNamespace() {
    Map<String, String> confMap = new HashMap<String, String>();
    confMap.put("spark.ssl.rpc.enabled", "true");
    confMap.put("spark.ssl.rpc.trustStoreReloadingEnabled", "false");
    confMap.put("spark.ssl.rpc.openSslEnabled", "false");
    confMap.put("spark.ssl.rpc.trustStoreReloadIntervalMs", "10000");
    confMap.put("spark.ssl.rpc.keyStore", SslSampleConfigs.keyStorePath);
    confMap.put("spark.ssl.rpc.keyStorePassword", "password");
    confMap.put("spark.ssl.rpc.privateKey", SslSampleConfigs.privateKeyPath);
    confMap.put("spark.ssl.rpc.keyPassword", "password");
    confMap.put("spark.ssl.rpc.privateKeyPassword", "password");
    confMap.put("spark.ssl.rpc.certChain", SslSampleConfigs.certChainPath);
    confMap.put("spark.ssl.rpc.trustStore", SslSampleConfigs.trustStorePath);
    confMap.put("spark.ssl.rpc.trustStorePassword", "password");
    return confMap;
  }

  /**
   * Create ConfigProvider based on the method above
   */
  public static ConfigProvider createDefaultConfigProviderForRpcNamespace() {
    return new MapConfigProvider(createDefaultConfigMapForRpcNamespace());
  }

  /**
   * Create ConfigProvider based on the method above
   */
  public static ConfigProvider createDefaultConfigProviderForRpcNamespaceWithAdditionalEntries(
      Map<String, String> entries) {
    Map<String, String> confMap = createDefaultConfigMapForRpcNamespace();
    confMap.putAll(entries);
    return new MapConfigProvider(confMap);
  }

  public static void createTrustStore(
    File trustStore, String password, String alias, Certificate cert)
    throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setCertificateEntry(alias, cert);
    saveKeyStore(ks, trustStore, password);
  }

  /**
   * Creates a keystore with multiple keys and saves it to a file.
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
  public static X509Certificate generateCertificate(
      String dn, KeyPair pair, int days, String algorithm)
      throws CertificateEncodingException, InvalidKeyException, IllegalStateException,
      NoSuchAlgorithmException, SignatureException {

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000L);
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
    // Write the file atomically to ensure tests don't read a partial write
    File tempFile = File.createTempFile("temp-key-store", "jks");
    FileOutputStream out = new FileOutputStream(tempFile);
    try {
      ks.store(out, password.toCharArray());
      out.close();
      Files.move(
        tempFile.toPath(),
        keyStore.toPath(),
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      );
    } finally {
      out.close();
    }
  }

  public static String getAbsolutePath(String path) {
    try {
      return new File(SslSampleConfigs.class.getResource(path).getFile()).getCanonicalPath();
    } catch (IOException e) {
       throw new RuntimeException("Failed to resolve path " + path, e);
    }
  }
}
