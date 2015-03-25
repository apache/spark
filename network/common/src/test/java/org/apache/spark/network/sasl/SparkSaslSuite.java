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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

/**
 * Jointly tests SparkSaslClient and SparkSaslServer, as both are black boxes.
 */
public class SparkSaslSuite {

  /** Provides a secret key holder which returns secret key == appId */
  private SecretKeyHolder secretKeyHolder = new SecretKeyHolder() {
    @Override
    public String getSaslUser(String appId) {
      return "user";
    }

    @Override
    public String getSecretKey(String appId) {
      return appId;
    }
  };

  @Test
  public void testMatching() {
    SparkSaslClient client = new SparkSaslClient("shared-secret", secretKeyHolder, false);
    SparkSaslServer server = new SparkSaslServer("shared-secret", secretKeyHolder);

    assertFalse(client.isComplete());
    assertFalse(server.isComplete());

    byte[] clientMessage = client.firstToken();

    while (!client.isComplete()) {
      clientMessage = client.response(server.response(clientMessage));
    }
    assertTrue(server.isComplete());

    // Disposal should invalidate
    server.dispose();
    assertFalse(server.isComplete());
    client.dispose();
    assertFalse(client.isComplete());
  }

  @Test
  public void testNonMatching() {
    SparkSaslClient client = new SparkSaslClient("my-secret", secretKeyHolder, false);
    SparkSaslServer server = new SparkSaslServer("your-secret", secretKeyHolder);

    assertFalse(client.isComplete());
    assertFalse(server.isComplete());

    byte[] clientMessage = client.firstToken();

    try {
      while (!client.isComplete()) {
        clientMessage = client.response(server.response(clientMessage));
      }
      fail("Should not have completed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Mismatched response"));
      assertFalse(client.isComplete());
      assertFalse(server.isComplete());
    }
  }

  @Test
  public void testSaslAuthentication() throws Exception {
    testSasl(false);
  }

  @Test
  public void testSaslEncryption() throws Exception {
    testSasl(true);
  }

  private void testSasl(boolean encrypt) throws Exception {
    TransportConf conf = new TransportConf(new SystemPropertyConfigProvider());

    RpcHandler rpcHandler = mock(RpcHandler.class);
    doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) {
          byte[] message = (byte[]) invocation.getArguments()[1];
          RpcResponseCallback cb = (RpcResponseCallback) invocation.getArguments()[2];
          assertEquals("Ping", new String(message, UTF_8));
          cb.onSuccess("Pong".getBytes(UTF_8));
          return null;
        }
      })
      .when(rpcHandler)
      .receive(any(TransportClient.class), any(byte[].class), any(RpcResponseCallback.class));

    SecretKeyHolder keyHolder = mock(SecretKeyHolder.class);
    when(keyHolder.getSaslUser(anyString())).thenReturn("user");
    when(keyHolder.getSecretKey(anyString())).thenReturn("secret");

    TransportContext ctx = new TransportContext(conf, rpcHandler);
    TransportServer server = null;
    TransportClient client = null;
    try {
      TransportServerBootstrap serverBootstrap = spy(new SaslServerBootstrap(keyHolder));
      server = ctx.createServer(Arrays.asList(serverBootstrap));

      TransportClientBootstrap clientBootstrap =
        new SaslClientBootstrap(conf, "user", keyHolder, encrypt);
      client = ctx.createClientFactory(Arrays.asList(clientBootstrap))
        .createClient(TestUtils.getLocalHost(), server.getPort());

      verify(serverBootstrap, times(1)).doBootstrap(any(Channel.class), any(RpcHandler.class));

      byte[] response = client.sendRpcSync("Ping".getBytes(UTF_8), TimeUnit.SECONDS.toMillis(10));
      assertEquals("Pong", new String(response, UTF_8));
    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.close();
      }
    }
  }

}
