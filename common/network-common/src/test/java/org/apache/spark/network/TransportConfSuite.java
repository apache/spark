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

import java.io.File;
import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.ssl.SslSampleConfigs;

public class TransportConfSuite {

  private TransportConf transportConf =
    new TransportConf(
     "shuffle", SslSampleConfigs.createDefaultConfigProviderForRpcNamespace());

  @Test
  public void testKeyStorePath() {
    assertEquals(new File(SslSampleConfigs.keyStorePath), transportConf.sslRpcKeyStore());
  }

  @Test
  public void testPrivateKeyPath() {
    assertEquals(new File(SslSampleConfigs.privateKeyPath), transportConf.sslRpcPrivateKey());
  }

  @Test
  public void testCertChainPath() {
    assertEquals(new File(SslSampleConfigs.certChainPath), transportConf.sslRpcCertChain());
  }

  @Test
  public void testTrustStorePath() {
    assertEquals(new File(SslSampleConfigs.trustStorePath), transportConf.sslRpcTrustStore());
  }

  @Test
  public void testTrustStoreReloadingEnabled() {
    assertFalse(transportConf.sslRpcTrustStoreReloadingEnabled());
  }

  @Test
  public void testOpenSslEnabled() {
    assertFalse(transportConf.sslRpcOpenSslEnabled());
  }

  @Test
  public void testSslRpcEnabled() {
    assertTrue(transportConf.sslRpcEnabled());
  }


  @Test
  public void testSslKeyStorePassword() {
    assertEquals("password", transportConf.sslRpcKeyStorePassword());
  }

  @Test
  public void testSslKeyPassword() {
    assertEquals("password", transportConf.sslRpcKeyPassword());
  }

  @Test
  public void testSslTrustStorePassword() {
    assertEquals("password", transportConf.sslRpcTrustStorePassword());
  }

  @Test
  public void testSsltrustStoreReloadIntervalMs() {
    assertEquals(10000, transportConf.sslRpctrustStoreReloadIntervalMs());
  }

  @Test
  public void testDefaultIOMode() {
    TransportConf c1 = new TransportConf("m1", new MapConfigProvider(Map.of()));
    assertEquals("AUTO", c1.ioMode());

    TransportConf c2 = new TransportConf("m1",
      new MapConfigProvider(Map.of("spark.io.mode.default", "KQUEUE")));
    assertEquals("KQUEUE", c2.ioMode());

    TransportConf c3 = new TransportConf("m2",
      new MapConfigProvider(Map.of("spark.io.mode.default", "KQUEUE")));
    assertEquals("KQUEUE", c3.ioMode());

    TransportConf c4 = new TransportConf("m3",
      new MapConfigProvider(Map.of(
        "spark.io.mode.default", "KQUEUE",
        "spark.m3.io.mode", "EPOLL")));
    assertEquals("EPOLL", c4.ioMode());
  }
}
