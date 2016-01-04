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

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.network.util.ssl.SslSampleConfigs.*;

/**
 *
 */
public class TestReloadingX509TrustManager {

  /**
   * Tests to ensure that loading a missing trust-store fails
   *
   * @throws Exception
   */
  @Test(expected = IOException.class)
  public void testLoadMissingTrustStore() throws Exception {
    File trustStore = new File("testmissing.jks");
    assertFalse(trustStore.exists());

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager(KeyStore.getDefaultType(), trustStore, "password", 10);
    try {
      tm.init();
    } finally {
      tm.destroy();
    }
  }

  /**
   * Tests to ensure that loading a corrupt trust-store fails
   *
   * @throws Exception
   */
  @Test(expected = IOException.class)
  public void testLoadCorruptTrustStore() throws Exception {
    File corruptStore = File.createTempFile("truststore-corrupt", "jks");
    corruptStore.deleteOnExit();
    OutputStream os = new FileOutputStream(corruptStore);
    os.write(1);
    os.close();

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager(KeyStore.getDefaultType(), corruptStore, "password", 10);
    try {
      tm.init();
    } finally {
      tm.destroy();
    }
  }

  /**
   * @throws Exception
   */
  @Test
  public void testReload() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    X509Certificate cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    X509Certificate cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    File trustStore = File.createTempFile("testreload", "jks");
    trustStore.deleteOnExit();
    createTrustStore(trustStore, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", trustStore, "password", 10);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 1000));

      // Add another cert
      Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
      certs.put("cert1", cert1);
      certs.put("cert2", cert2);
      createTrustStore(trustStore, "password", certs);

      // and wait to be sure reload has taken place
      assertEquals(10, tm.getReloadInterval());

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 200));

      assertEquals(2, tm.getAcceptedIssuers().length);
    } finally {
      tm.destroy();
    }
  }

  /**
   * @throws Exception
   */
  @Test
  public void testReloadMissingTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    X509Certificate cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    X509Certificate cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    File trustStore = new File("testmissing.jks");
    trustStore.deleteOnExit();
    assertFalse(trustStore.exists());
    createTrustStore(trustStore, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", trustStore, "password", 10);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      X509Certificate cert = tm.getAcceptedIssuers()[0];
      trustStore.delete();

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 200));

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      tm.destroy();
    }
  }

  /**
   * @throws Exception
   */
  @Test
  public void testReloadCorruptTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    X509Certificate cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    X509Certificate cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    File corruptStore = File.createTempFile("truststore-corrupt", "jks");
    corruptStore.deleteOnExit();
    createTrustStore(corruptStore, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", corruptStore, "password", 10);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      X509Certificate cert = tm.getAcceptedIssuers()[0];

      OutputStream os = new FileOutputStream(corruptStore);
      os.write(1);
      os.close();
      corruptStore.setLastModified(System.currentTimeMillis() - 1000);

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 200));

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      tm.destroy();
    }
  }
}
