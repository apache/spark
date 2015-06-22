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

package org.apache.spark.network.sasl;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class SaslIntegrationSuite {
  static ExternalShuffleBlockHandler handler;
  static TransportServer server;
  static TransportConf conf;
  static TransportContext context;

  TransportClientFactory clientFactory;

  /** Provides a secret key holder which always returns the given secret key. */
  static class TestSecretKeyHolder implements SecretKeyHolder {

    private final String secretKey;

    TestSecretKeyHolder(String secretKey) {
      this.secretKey = secretKey;
    }

    @Override
    public String getSaslUser(String appId) {
      return "user";
    }
    @Override
    public String getSecretKey(String appId) {
      return secretKey;
    }
  }


  @BeforeClass
  public static void beforeAll() throws IOException {
    SecretKeyHolder secretKeyHolder = new TestSecretKeyHolder("good-key");
    conf = new TransportConf(new SystemPropertyConfigProvider());
    context = new TransportContext(conf, new TestRpcHandler());

    TransportServerBootstrap bootstrap = new SaslServerBootstrap(conf, secretKeyHolder);
    server = context.createServer(Arrays.asList(bootstrap));
  }


  @AfterClass
  public static void afterAll() {
    server.close();
  }

  @After
  public void afterEach() {
    if (clientFactory != null) {
      clientFactory.close();
      clientFactory = null;
    }
  }

  @Test
  public void testGoodClient() throws IOException {
    clientFactory = context.createClientFactory(
      Lists.<TransportClientBootstrap>newArrayList(
        new SaslClientBootstrap(conf, "app-id", new TestSecretKeyHolder("good-key"))));

    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    String msg = "Hello, World!";
    byte[] resp = client.sendRpcSync(msg.getBytes(), 1000);
    assertEquals(msg, new String(resp)); // our rpc handler should just return the given msg
  }

  @Test
  public void testBadClient() {
    clientFactory = context.createClientFactory(
      Lists.<TransportClientBootstrap>newArrayList(
        new SaslClientBootstrap(conf, "app-id", new TestSecretKeyHolder("bad-key"))));

    try {
      // Bootstrap should fail on startup.
      clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Mismatched response"));
    }
  }

  @Test
  public void testNoSaslClient() throws IOException {
    clientFactory = context.createClientFactory(
      Lists.<TransportClientBootstrap>newArrayList());

    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      client.sendRpcSync(new byte[13], 1000);
      fail("Should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Expected SaslMessage"));
    }

    try {
      // Guessing the right tag byte doesn't magically get you in...
      client.sendRpcSync(new byte[] { (byte) 0xEA }, 1000);
      fail("Should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("java.lang.IndexOutOfBoundsException"));
    }
  }

  @Test
  public void testNoSaslServer() {
    RpcHandler handler = new TestRpcHandler();
    TransportContext context = new TransportContext(conf, handler);
    clientFactory = context.createClientFactory(
      Lists.<TransportClientBootstrap>newArrayList(
        new SaslClientBootstrap(conf, "app-id", new TestSecretKeyHolder("key"))));
    TransportServer server = context.createServer();
    try {
      clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Digest-challenge format violation"));
    } finally {
      server.close();
    }
  }

  /** RPC handler which simply responds with the message it received. */
  public static class TestRpcHandler extends RpcHandler {
    @Override
    public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
      callback.onSuccess(message);
    }

    @Override
    public StreamManager getStreamManager() {
      return new OneForOneStreamManager();
    }
  }
}
