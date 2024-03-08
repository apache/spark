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

import io.netty.buffer.ByteBufAllocator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import static org.apache.spark.network.ssl.SslSampleConfigs.*;

public class SSLFactorySuite {

  @Test
  public void testBuildWithJKSAndPEMWithPasswords() throws Exception {
    SSLFactory factory = new SSLFactory.Builder()
      .openSslEnabled(true)
      .requestedProtocol("TLSv1.3")
      .keyStore(new File(SslSampleConfigs.keyStorePath), "password")
      .privateKey(new File(SslSampleConfigs.privateKeyPath))
      .privateKeyPassword("password")
      .keyPassword("password")
      .certChain(new File(SslSampleConfigs.certChainPath))
      .trustStore(
        new File(SslSampleConfigs.trustStorePath),
        "password",
        true,
        10000
      )
      .build();
    factory.createSSLEngine(true, ByteBufAllocator.DEFAULT);
  }

  @Test
  public void testBuildWithJKSAndPEMWithOnlyJKSPassword() throws Exception {
    SSLFactory factory = new SSLFactory.Builder()
      .openSslEnabled(true)
      .requestedProtocol("TLSv1.3")
      .keyStore(new File(SslSampleConfigs.keyStorePath), "password")
      .privateKey(new File(SslSampleConfigs.unencryptedPrivateKeyPath))
      .keyPassword("password")
      .certChain(new File(SslSampleConfigs.unencryptedCertChainPath))
      .trustStore(
        new File(SslSampleConfigs.trustStorePath),
        "password",
        true,
        10000
      )
      .build();
    factory.createSSLEngine(true, ByteBufAllocator.DEFAULT);
  }

  @Test
  public void testBuildWithJKSKeyStorePassword() throws Exception {
    // if key password is null, fall back to keystore password
    SSLFactory factory = new SSLFactory.Builder()
      .requestedProtocol("TLSv1.3")
      .keyStore(new File(SslSampleConfigs.keyStorePath), "password")
      .trustStore(
        new File(SslSampleConfigs.trustStorePath),
        "password",
        true,
        10000
      )
      .build();
    factory.createSSLEngine(true, ByteBufAllocator.DEFAULT);
  }

  @Test
  public void testKeyAndKeystorePasswordsAreDistinct() throws Exception {
    assertThrows(RuntimeException.class, () -> {
      // Set the wrong password, validate we fail
      SSLFactory factory = new SSLFactory.Builder()
        .requestedProtocol("TLSv1.3")
        .keyStore(new File(SslSampleConfigs.keyStorePath), "password")
        .keyPassword("wrong")
        .trustStore(
          new File(SslSampleConfigs.trustStorePath),
          "password",
          true,
          10000
        )
        .build();
      factory.createSSLEngine(true, ByteBufAllocator.DEFAULT);
    });
  }
}
