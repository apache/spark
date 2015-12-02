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
package org.apache.spark.network;

import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.ssl.SslSampleConfigs;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 *
 */
public class TransportConfSuite {

  private TransportConf transportConf =
    new TransportConf("shuffle", SslSampleConfigs.createDefaultConfigProvider());

  /**
   *
   */
  @Test
  public void testKeyStorePath() {
    assertEquals(new File(SslSampleConfigs.keyStorePath), transportConf.sslShuffleKeyStore());
  }

  /**
   *
   */
  @Test
  public void testPrivateKeyPath() {
    assertEquals(new File(SslSampleConfigs.privateKeyPath), transportConf.sslShufflePrivateKey());
  }

  /**
   *
   */
  @Test
  public void testCertChainPath() {
    assertEquals(new File(SslSampleConfigs.certChainPath), transportConf.sslShuffleCertChain());
  }

  /**
   *
   */
  @Test
  public void testTrustStorePath() {
    assertEquals(new File(SslSampleConfigs.trustStorePath), transportConf.sslShuffleTrustStore());
  }

  /**
   *
   */
  @Test
  public void testTrustStoreReloadingEnabled() {
    assertFalse(transportConf.sslShuffleTrustStoreReloadingEnabled());
  }

  /**
   *
   */
  @Test
  public void testOpenSslEnabled() {
    assertFalse(transportConf.sslShuffleOpenSslEnabled());
  }

  /**
   *
   */
  @Test
  public void testSslShuffleEnabled() {
    assertTrue(transportConf.sslShuffleEnabled());
  }

  /**
   *
   */
  @Test
  public void testSslKeyStorePassword() {
    assertEquals("password", transportConf.sslShuffleKeyStorePassword());
  }

  /**
   *
   */
  @Test
  public void testSslKeyPassword() {
    assertEquals("password", transportConf.sslShuffleKeyPassword());
  }

  /**
   *
   */
  @Test
  public void testSslTrustStorePassword() {
    assertEquals("password", transportConf.sslShuffleTrustStorePassword());
  }

  /**
   *
   */
  @Test
  public void testSslTrustStoreReloadInterval() {
    assertEquals(10000, transportConf.sslShuffleTrustStoreReloadInterval());
  }
}
