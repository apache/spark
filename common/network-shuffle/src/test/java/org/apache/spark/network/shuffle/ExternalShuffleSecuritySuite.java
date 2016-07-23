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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ExternalShuffleSecuritySuite {

  TransportConf conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
  TransportServer server;

  @Before
  public void beforeEach() throws IOException {
    TransportContext context =
      new TransportContext(conf, new ExternalShuffleBlockHandler(conf, null));
    TransportServerBootstrap bootstrap = new SaslServerBootstrap(conf,
        new TestSecretKeyHolder("my-app-id", "secret"));
    this.server = context.createServer(Arrays.asList(bootstrap));
  }

  @After
  public void afterEach() {
    if (server != null) {
      server.close();
      server = null;
    }
  }

  @Test
  public void testValid() throws IOException {
    validate("my-app-id", "secret", false);
  }

  @Test
  public void testBadAppId() {
    try {
      validate("wrong-app-id", "secret", false);
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Wrong appId!"));
    }
  }

  @Test
  public void testBadSecret() {
    try {
      validate("my-app-id", "bad-secret", false);
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Mismatched response"));
    }
  }

  @Test
  public void testEncryption() throws IOException {
    validate("my-app-id", "secret", true);
  }

  /** Creates an ExternalShuffleClient and attempts to register with the server. */
  private void validate(String appId, String secretKey, boolean encrypt) throws IOException {
    ExternalShuffleClient client =
      new ExternalShuffleClient(conf, new TestSecretKeyHolder(appId, secretKey), true, encrypt);
    client.init(appId);
    // Registration either succeeds or throws an exception.
    client.registerWithShuffleServer(TestUtils.getLocalHost(), server.getPort(), "exec0",
      new ExecutorShuffleInfo(new String[0], 0,
        "org.apache.spark.shuffle.sort.SortShuffleManager"));
    client.close();
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
