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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * A {@link TrustManager} implementation that reloads its configuration when
 * the truststore file on disk changes.
 * This implementation is based entirely on the org.apache.hadoop.security.ssl.ReloadingX509TrustManager
 * class in the Apache Hadoop Encrypted Shuffle implementation.
 *
 * @see <a href="http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/EncryptedShuffle.html">Hadoop MapReduce Next Generation - Encrypted Shuffle</a>
 */
public final class ReloadingX509TrustManager
  implements X509TrustManager, Runnable {

  private final Logger logger = LoggerFactory.getLogger(ReloadingX509TrustManager.class);

  private String type;
  private File file;
  private String password;
  private long lastLoaded;
  private long reloadInterval;
  private AtomicReference<X509TrustManager> trustManagerRef;

  private volatile boolean running;
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
    file = trustStore;
    this.password = password;
    trustManagerRef = new AtomicReference<X509TrustManager>();
    trustManagerRef.set(loadTrustManager());
    this.reloadInterval = reloadInterval;
  }

  /**
   * Starts the reloader thread.
   */
  public void init() {
    reloader = new Thread(this, "Truststore reloader thread");
    reloader.setDaemon(true);
    running = true;
    reloader.start();
  }

  /**
   * Stops the reloader thread.
   */
  public void destroy() {
    running = false;
    reloader.interrupt();
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
        chain[0].toString());
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
        chain[0].toString());
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

  boolean needsReload() {
    boolean reload = true;
    if (file.exists()) {
      if (file.lastModified() == lastLoaded) {
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
    lastLoaded = file.lastModified();
    FileInputStream in = new FileInputStream(file);
    try {
      ks.load(in, password.toCharArray());
      logger.debug("Loaded truststore '" + file + "'");
    } finally {
      in.close();
    }

    TrustManagerFactory trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(ks);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    for (TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof X509TrustManager) {
        trustManager = (X509TrustManager) trustManager1;
        break;
      }
    }
    return trustManager;
  }

  @Override
  public void run() {
    while (running) {
      try {
        Thread.sleep(reloadInterval);
      } catch (InterruptedException e) {
        //NOP
      }
      if (running && needsReload()) {
        try {
          trustManagerRef.set(loadTrustManager());
        } catch (Exception ex) {
          logger.warn("Could not load truststore (keep using existing one) : " + ex.toString(), ex);
        }
      }
    }
  }
}