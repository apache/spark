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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ExternalShuffleSecuritySuite {

  TransportConf conf = createTransportConf(false);
  TransportServer server;
  TransportContext transportContext;

  protected TransportConf createTransportConf(boolean encrypt) {
    if (encrypt) {
      return new TransportConf("shuffle", new MapConfigProvider(
        ImmutableMap.of("spark.authenticate.enableSaslEncryption", "true")));
    } else {
      return new TransportConf("shuffle", MapConfigProvider.EMPTY);
    }
  }

  @BeforeEach
  public void beforeEach() throws IOException {
    transportContext = new TransportContext(conf, new ExternalBlockHandler(conf, null));
    TransportServerBootstrap bootstrap = new SaslServerBootstrap(conf,
        new TestSecretKeyHolder("my-app-id", "secret"));
    this.server = transportContext.createServer(Arrays.asList(bootstrap));
  }

  @AfterEach
  public void afterEach() {
    if (server != null) {
      server.close();
      server = null;
    }
    if (transportContext != null) {
      transportContext.close();
      transportContext = null;
    }
  }

  @Test
  public void testValid() throws IOException, InterruptedException {
    validate("my-app-id", "secret", false);
  }

  @Test
  public void testBadAppId() {
    Exception e = assertThrows(Exception.class,
      () -> validate("wrong-app-id", "secret", false));
    assertTrue(e.getMessage().contains("Wrong appId!"), e.getMessage());
  }

  @Test
  public void testBadSecret() {
    Exception e = assertThrows(Exception.class,
      () -> validate("my-app-id", "bad-secret", false));
    assertTrue(e.getMessage().contains("Mismatched response"), e.getMessage());
  }

  @Test
  public void testEncryption() throws IOException, InterruptedException {
    validate("my-app-id", "secret", true);
  }

  /** Creates an ExternalBlockStoreClient and attempts to register with the server. */
  private void validate(String appId, String secretKey, boolean encrypt)
        throws IOException, InterruptedException {
    TransportConf testConf = conf;
    if (encrypt) {
      testConf = createTransportConf(encrypt);
    }

    try (ExternalBlockStoreClient client =
        new ExternalBlockStoreClient(
          testConf, new TestSecretKeyHolder(appId, secretKey), true, 5000)) {
      client.init(appId);
      // Registration either succeeds or throws an exception.
      client.registerWithShuffleServer(TestUtils.getLocalHost(), server.getPort(), "exec0",
        new ExecutorShuffleInfo(
          new String[0], 0, "org.apache.spark.shuffle.sort.SortShuffleManager")
      );
    }
  }

  /** Provides a secret key holder which always returns the given secret key, for a single appId. */
  static class TestSecretKeyHolder implements SecretKeyHolder {
    private final String appId;
    private final String secretKey;

    TestSecretKeyHolder(String appId, String secretKey) {
      this.appId = appId;
      this.secretKey = secretKey;
    }

    @Override
    public String getSaslUser(String appId) {
      return "user";
    }

    @Override
    public String getSecretKey(String appId) {
      if (!appId.equals(this.appId)) {
        throw new IllegalArgumentException("Wrong appId!");
      }
      return secretKey;
    }
  }
}
