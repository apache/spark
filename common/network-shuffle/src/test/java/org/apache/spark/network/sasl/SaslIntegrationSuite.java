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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class SaslIntegrationSuite {

  // Use a long timeout to account for slow / overloaded build machines. In the normal case,
  // tests should finish way before the timeout expires.
  private static final long TIMEOUT_MS = 10_000;

  static TransportServer server;
  static TransportConf conf;
  static TransportContext context;
  static SecretKeyHolder secretKeyHolder;

  TransportClientFactory clientFactory;

  @BeforeClass
  public static void beforeAll() throws IOException {
    conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    context = new TransportContext(conf, new TestRpcHandler());

    secretKeyHolder = mock(SecretKeyHolder.class);
    when(secretKeyHolder.getSaslUser(eq("app-1"))).thenReturn("app-1");
    when(secretKeyHolder.getSecretKey(eq("app-1"))).thenReturn("app-1");
    when(secretKeyHolder.getSaslUser(eq("app-2"))).thenReturn("app-2");
    when(secretKeyHolder.getSecretKey(eq("app-2"))).thenReturn("app-2");
    when(secretKeyHolder.getSaslUser(anyString())).thenReturn("other-app");
    when(secretKeyHolder.getSecretKey(anyString())).thenReturn("correct-password");

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
  public void testGoodClient() throws IOException, InterruptedException {
    clientFactory = context.createClientFactory(
        Arrays.asList(new SaslClientBootstrap(conf, "app-1", secretKeyHolder)));

    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    String msg = "Hello, World!";
    ByteBuffer resp = client.sendRpcSync(JavaUtils.stringToBytes(msg), TIMEOUT_MS);
    assertEquals(msg, JavaUtils.bytesToString(resp));
  }

  @Test
  public void testBadClient() {
    SecretKeyHolder badKeyHolder = mock(SecretKeyHolder.class);
    when(badKeyHolder.getSaslUser(anyString())).thenReturn("other-app");
    when(badKeyHolder.getSecretKey(anyString())).thenReturn("wrong-password");
    clientFactory = context.createClientFactory(
        Arrays.asList(new SaslClientBootstrap(conf, "unknown-app", badKeyHolder)));

    try {
      // Bootstrap should fail on startup.
      clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
      fail("Connection should have failed.");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Mismatched response"));
    }
  }

  @Test
  public void testNoSaslClient() throws IOException, InterruptedException {
    clientFactory = context.createClientFactory(new ArrayList<>());

    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      client.sendRpcSync(ByteBuffer.allocate(13), TIMEOUT_MS);
      fail("Should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Expected SaslMessage"));
    }

    try {
      // Guessing the right tag byte doesn't magically get you in...
      client.sendRpcSync(ByteBuffer.wrap(new byte[] { (byte) 0xEA }), TIMEOUT_MS);
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
      Arrays.asList(new SaslClientBootstrap(conf, "app-1", secretKeyHolder)));
    try (TransportServer server = context.createServer()) {
      clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Digest-challenge format violation"));
    }
  }

  /** RPC handler which simply responds with the message it received. */
  public static class TestRpcHandler extends RpcHandler {
    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
      callback.onSuccess(message);
    }

    @Override
    public StreamManager getStreamManager() {
      return new OneForOneStreamManager();
    }
  }
}
