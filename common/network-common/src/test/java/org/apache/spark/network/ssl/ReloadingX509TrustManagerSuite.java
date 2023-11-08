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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import static org.apache.spark.network.ssl.SslSampleConfigs.*;

public class ReloadingX509TrustManagerSuite {

  /**
   * Waits until reload count hits the requested value, sleeping 100ms at a time.
   * If the maximum number of attempts is hit, throws a RuntimeException
   * @param tm the trust manager to wait for
   * @param count The count to wait for
   * @param attempts The number of attempts to wait for
   */
  private void waitForReloadCount(ReloadingX509TrustManager tm, int count, int attempts)
          throws InterruptedException {
    if (tm.reloadCount > count) {
      throw new IllegalStateException(
        "Passed invalid count " + count + " to waitForReloadCount, already have " + tm.reloadCount);
    }
    for (int i = 0; i < attempts; i++) {
      if (tm.reloadCount >= count) {
        return;
      }
      // Adapted from SystemClock.waitTillTime
      long startTime = System.currentTimeMillis();
      long targetTime = startTime + 100;
      long currentTime = startTime;
      while (currentTime < targetTime) {
        long sleepTime = Math.min(10, targetTime - currentTime);
        Thread.sleep(sleepTime);
        currentTime = System.currentTimeMillis();
      }
    }
    throw new IllegalStateException("Trust store not reloaded after " + attempts + " attempts!");
  }

  /**
   * Waits until we make some number of attempts to reload, and verifies
   * that the actual reload count did not change
   *
   * @param tm the trust manager to wait for
   * @param attempts The number of attempts to wait for
   */
  private void waitForNoReload(ReloadingX509TrustManager tm, int attempts)
          throws InterruptedException {
    int oldReloadCount = tm.reloadCount;
    int checkCount = tm.needsReloadCheckCounts;
    int target = checkCount + attempts;
    while (checkCount < target) {
      Thread.sleep(100);
      checkCount = tm.needsReloadCheckCounts;
    }
    assertEquals(oldReloadCount, tm.reloadCount);
  }

  /**
   * Tests to ensure that loading a missing trust-store fails
   *
   * @throws Exception
   */
  @Test
  public void testLoadMissingTrustStore() throws Exception {
    File trustStore = new File("testmissing.jks");
    assertFalse(trustStore.exists());

    assertThrows(IOException.class, () -> {
      ReloadingX509TrustManager tm = new ReloadingX509TrustManager(
        KeyStore.getDefaultType(),
        trustStore,
        "password",
        10
      );
      try {
        tm.init();
      } finally {
        tm.destroy();
      }
    });
  }

  /**
   * Tests to ensure that loading a corrupt trust-store fails
   *
   * @throws Exception
   */
  @Test
  public void testLoadCorruptTrustStore() throws Exception {
    File corruptStore = File.createTempFile("truststore-corrupt", "jks");
    corruptStore.deleteOnExit();
    OutputStream os = new FileOutputStream(corruptStore);
    os.write(1);
    os.close();

    assertThrows(IOException.class, () -> {
      ReloadingX509TrustManager tm = new ReloadingX509TrustManager(
        KeyStore.getDefaultType(),
        corruptStore,
        "password",
        10
      );
      try {
        tm.init();
      } finally {
        tm.destroy();
        corruptStore.delete();
      }
    });
  }

  /**
   * Tests that we successfully reload when a file is updated
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
      new ReloadingX509TrustManager("jks", trustStore, "password", 1);
    assertEquals(1, tm.getReloadInterval());
    assertEquals(0, tm.reloadCount);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      // At this point we haven't reloaded, just the initial load
      assertEquals(0, tm.reloadCount);

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 1000));

      // Add another cert
      Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
      certs.put("cert1", cert1);
      certs.put("cert2", cert2);
      createTrustStore(trustStore, "password", certs);

      // Wait up to 10s until we reload
      waitForReloadCount(tm, 1, 100);

      assertEquals(2, tm.getAcceptedIssuers().length);
    } finally {
      tm.destroy();
      trustStore.delete();
    }
  }

  /**
   * Tests that we keep old certs if the trust store goes missing
   *
   * @throws Exception
   */
  @Test
  public void testReloadMissingTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    X509Certificate cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    File trustStore = new File("testmissing.jks");
    trustStore.deleteOnExit();
    assertFalse(trustStore.exists());
    createTrustStore(trustStore, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", trustStore, "password", 1);
    assertEquals(0, tm.reloadCount);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      X509Certificate cert = tm.getAcceptedIssuers()[0];
      trustStore.delete();

      // Wait for up to 5s - we should *not* reload
      waitForNoReload(tm, 50);

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      tm.destroy();
    }
  }

  /**
   * Tests that we keep old certs if the new truststore is corrupt
   * @throws Exception
   */
  @Test
  public void testReloadCorruptTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    X509Certificate cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    File corruptStore = File.createTempFile("truststore-corrupt", "jks");
    corruptStore.deleteOnExit();
    createTrustStore(corruptStore, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", corruptStore, "password", 1);
    assertEquals(0, tm.reloadCount);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      X509Certificate cert = tm.getAcceptedIssuers()[0];

      OutputStream os = new FileOutputStream(corruptStore);
      os.write(1);
      os.close();
      corruptStore.setLastModified(System.currentTimeMillis() - 1000);

      // Wait for up to 5s - we should *not* reload
      waitForNoReload(tm, 50);

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      tm.destroy();
      corruptStore.delete();
    }
  }

  /**
   * Tests that we successfully reload when the trust store is a symlink
   * and we update the contents of the pointed-to file or we update the file it points to.
   * @throws Exception
   */
  @Test
  public void testReloadSymlink() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    X509Certificate cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    X509Certificate cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    X509Certificate cert3 = generateCertificate("CN=Cert3", kp, 30, "SHA1withRSA");

    File trustStore1 = File.createTempFile("testreload", "jks");
    trustStore1.deleteOnExit();
    createTrustStore(trustStore1, "password", "cert1", cert1);

    File trustStore2 = File.createTempFile("testreload", "jks");
    Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
    certs.put("cert1", cert1);
    certs.put("cert2", cert2);
    createTrustStore(trustStore2, "password", certs);

    File trustStoreSymlink = File.createTempFile("testreloadsymlink", "jks");
    trustStoreSymlink.delete();
    Files.createSymbolicLink(trustStoreSymlink.toPath(), trustStore1.toPath());

    ReloadingX509TrustManager tm =
            new ReloadingX509TrustManager("jks", trustStoreSymlink, "password", 1);
    assertEquals(1, tm.getReloadInterval());
    assertEquals(0, tm.reloadCount);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      // At this point we haven't reloaded, just the initial load
      assertEquals(0, tm.reloadCount);

      // Repoint to trustStore2, which has another cert
      trustStoreSymlink.delete();
      Files.createSymbolicLink(trustStoreSymlink.toPath(), trustStore2.toPath());

      // Wait up to 10s until we reload
      waitForReloadCount(tm, 1, 100);

      assertEquals(2, tm.getAcceptedIssuers().length);

      // Add another cert
      certs.put("cert3", cert3);
      createTrustStore(trustStore2, "password", certs);

      // Wait up to 10s until we reload
      waitForReloadCount(tm, 2, 100);

      assertEquals(3, tm.getAcceptedIssuers().length);
    } finally {
      tm.destroy();
      trustStore1.delete();
      trustStore2.delete();
      trustStoreSymlink.delete();
    }
  }
}
