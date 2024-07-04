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

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * A {@link TrustManager} implementation that reloads its configuration when
 * the truststore file on disk changes.
 * This implementation is based off of the
 * org.apache.hadoop.security.ssl.ReloadingX509TrustManager class in the Apache Hadoop Encrypted
 * Shuffle implementation.
 *
 * @see <a href="https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/EncryptedShuffle.html">Hadoop MapReduce Next Generation - Encrypted Shuffle</a>
 */
public final class ReloadingX509TrustManager
        implements X509TrustManager, Runnable {

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(ReloadingX509TrustManager.class);

  private final String type;
  private final File file;
  // The file being pointed to by `file` if it's a link
  private String canonicalPath;
  private final String password;
  private long lastLoaded;
  private final long reloadInterval;
  @VisibleForTesting
  protected volatile int reloadCount;
  @VisibleForTesting
  protected volatile int needsReloadCheckCounts;
  private final AtomicReference<X509TrustManager> trustManagerRef;

  private Thread reloader;

  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying trustore file has changed.
   *
   * @param type           type of truststore file, typically 'jks'.
   * @param trustStore     the truststore file.
   * @param password       password of the truststore file.
   * @param reloadInterval interval to check if the truststore file has
   *                       changed, in milliseconds.
   * @throws IOException              thrown if the truststore could not be initialized due
   *                                  to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   *                                  initialized due to a security error.
   */
  public ReloadingX509TrustManager(
      String type, File trustStore, String password, long reloadInterval)
      throws IOException, GeneralSecurityException {
    this.type = type;
    this.file = trustStore;
    this.canonicalPath = this.file.getCanonicalPath();
    this.password = password;
    this.trustManagerRef = new AtomicReference<X509TrustManager>();
    this.trustManagerRef.set(loadTrustManager());
    this.reloadInterval = reloadInterval;
    this.reloadCount = 0;
    this.needsReloadCheckCounts = 0;
  }

  /**
   * Starts the reloader thread.
   */
  public void init() {
    reloader = new Thread(this, "Truststore reloader thread");
    reloader.setDaemon(true);
    reloader.start();
  }

  /**
   * Stops the reloader thread.
   */
  public void destroy() throws InterruptedException {
    reloader.interrupt();
    reloader.join();
  }

  /**
   * Returns the reload check interval.
   *
   * @return the reload check interval, in milliseconds.
   */
  public long getReloadInterval() {
    return reloadInterval;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
          throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkClientTrusted(chain, authType);
    } else {
      throw new CertificateException("Unknown client chain certificate: " +
        chain[0].toString() + ". Please ensure the correct trust store is specified in the config");
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
          throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkServerTrusted(chain, authType);
    } else {
      throw new CertificateException("Unknown server chain certificate: " +
        chain[0].toString() + ". Please ensure the correct trust store is specified in the config");
    }
  }

  private static final X509Certificate[] EMPTY = new X509Certificate[0];

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    X509Certificate[] issuers = EMPTY;
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      issuers = tm.getAcceptedIssuers();
    }
    return issuers;
  }

  boolean needsReload() throws IOException {
    boolean reload = true;
    File latestCanonicalFile = file.getCanonicalFile();
    if (file.exists() && latestCanonicalFile.exists()) {
      // `file` can be a symbolic link. We need to reload if it points to another file,
      // or if the file has been modified
      if (latestCanonicalFile.getPath().equals(canonicalPath) &&
          latestCanonicalFile.lastModified() == lastLoaded) {
        reload = false;
      }
    } else {
      lastLoaded = 0;
    }
    return reload;
  }

  X509TrustManager loadTrustManager()
          throws IOException, GeneralSecurityException {
    X509TrustManager trustManager = null;
    KeyStore ks = KeyStore.getInstance(type);
    File latestCanonicalFile = file.getCanonicalFile();
    canonicalPath = latestCanonicalFile.getPath();
    lastLoaded = latestCanonicalFile.lastModified();
    try (FileInputStream in = new FileInputStream(latestCanonicalFile)) {
      char[] passwordCharacters = password != null? password.toCharArray() : null;
      ks.load(in, passwordCharacters);
      logger.debug("Loaded truststore '" + file + "'");
    }

    TrustManagerFactory trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(ks);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    for (TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof X509TrustManager x509TrustManager) {
        trustManager = x509TrustManager;
        break;
      }
    }
    return trustManager;
  }

  @Override
  public void run() {
    boolean running = true;
    while (running) {
      try {
        Thread.sleep(reloadInterval);
      } catch (InterruptedException e) {
        running = false;
      }
      try {
        if (running && needsReload()) {
          try {
            trustManagerRef.set(loadTrustManager());
            this.reloadCount += 1;
          } catch (Exception ex) {
            logger.warn(
              "Could not load truststore (keep using existing one) : ",
              ex
            );
          }
        }
      } catch (IOException ex) {
       logger.warn("Could not check whether truststore needs reloading: ", ex);
      }
      needsReloadCheckCounts++;
    }
  }
}
