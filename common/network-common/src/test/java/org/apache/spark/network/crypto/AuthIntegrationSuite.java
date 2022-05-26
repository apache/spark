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

package org.apache.spark.network.crypto;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class AuthIntegrationSuite {

  private AuthTestCtx ctx;

  @After
  public void cleanUp() throws Exception {
    if (ctx != null) {
      ctx.close();
    }
    ctx = null;
  }

  @Test
  public void testNewAuth() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("secret");
    ctx.createClient("secret");

    ByteBuffer reply = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
    assertEquals("Pong", JavaUtils.bytesToString(reply));
    assertNull(ctx.authRpcHandler.saslHandler);
  }

  @Test
  public void testAuthFailure() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("server");

    assertThrows(Exception.class, () -> ctx.createClient("client"));
    assertFalse(ctx.authRpcHandler.isAuthenticated());
    assertFalse(ctx.serverChannel.isActive());
  }

  @Test
  public void testSaslServerFallback() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("secret", true);
    ctx.createClient("secret", false);

    ByteBuffer reply = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
    assertEquals("Pong", JavaUtils.bytesToString(reply));
    assertNotNull(ctx.authRpcHandler.saslHandler);
    assertTrue(ctx.authRpcHandler.isAuthenticated());
  }

  @Test
  public void testSaslClientFallback() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("secret", false);
    ctx.createClient("secret", true);

    ByteBuffer reply = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
    assertEquals("Pong", JavaUtils.bytesToString(reply));
  }

  @Test
  public void testAuthReplay() throws Exception {
    // This test covers the case where an attacker replays a challenge message sniffed from the
    // network, but doesn't know the actual secret. The server should close the connection as
    // soon as a message is sent after authentication is performed. This is emulated by removing
    // the client encryption handler after authentication.
    ctx = new AuthTestCtx();
    ctx.createServer("secret");
    ctx.createClient("secret");

    assertNotNull(ctx.client.getChannel().pipeline()
      .remove(TransportCipher.ENCRYPTION_HANDLER_NAME));
    assertThrows(Exception.class,
      () -> ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000));
    assertTrue(ctx.authRpcHandler.isAuthenticated());
  }

  @Test
  public void testLargeMessageEncryption() throws Exception {
    // Use a big length to create a message that cannot be put into the encryption buffer completely
    final int testErrorMessageLength = TransportCipher.STREAM_BUFFER_SIZE;
    ctx = new AuthTestCtx(new RpcHandler() {
      @Override
      public void receive(
          TransportClient client,
          ByteBuffer message,
          RpcResponseCallback callback) {
        char[] longMessage = new char[testErrorMessageLength];
        Arrays.fill(longMessage, 'D');
        callback.onFailure(new RuntimeException(new String(longMessage)));
      }

      @Override
      public StreamManager getStreamManager() {
        return null;
      }
    });
    ctx.createServer("secret");
    ctx.createClient("secret");

    Exception e = assertThrows(Exception.class,
      () -> ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000));
    assertTrue(ctx.authRpcHandler.isAuthenticated());
    assertTrue(e.getMessage() + " is not an expected error", e.getMessage().contains("DDDDD"));
    // Verify we receive the complete error message
    int messageStart = e.getMessage().indexOf("DDDDD");
    int messageEnd = e.getMessage().lastIndexOf("DDDDD") + 5;
    assertEquals(testErrorMessageLength, messageEnd - messageStart);
  }

  private static class AuthTestCtx {

    private final String appId = "testAppId";
    private final TransportConf conf;
    private final TransportContext ctx;

    TransportClient client;
    TransportServer server;
    volatile Channel serverChannel;
    volatile AuthRpcHandler authRpcHandler;

    AuthTestCtx() throws Exception {
      this(new RpcHandler() {
        @Override
        public void receive(
            TransportClient client,
            ByteBuffer message,
            RpcResponseCallback callback) {
          assertEquals("Ping", JavaUtils.bytesToString(message));
          callback.onSuccess(JavaUtils.stringToBytes("Pong"));
        }

        @Override
        public StreamManager getStreamManager() {
          return null;
        }
      });
    }

    AuthTestCtx(RpcHandler rpcHandler) throws Exception {
      Map<String, String> testConf = ImmutableMap.of("spark.network.crypto.enabled", "true");
      this.conf = new TransportConf("rpc", new MapConfigProvider(testConf));
      this.ctx = new TransportContext(conf, rpcHandler);
    }

    void createServer(String secret) throws Exception {
      createServer(secret, true);
    }

    void createServer(String secret, boolean enableAes) throws Exception {
      TransportServerBootstrap introspector = (channel, rpcHandler) -> {
        this.serverChannel = channel;
        if (rpcHandler instanceof AuthRpcHandler) {
          this.authRpcHandler = (AuthRpcHandler) rpcHandler;
        }
        return rpcHandler;
      };
      SecretKeyHolder keyHolder = createKeyHolder(secret);
      TransportServerBootstrap auth = enableAes ? new AuthServerBootstrap(conf, keyHolder)
        : new SaslServerBootstrap(conf, keyHolder);
      this.server = ctx.createServer(Arrays.asList(auth, introspector));
    }

    void createClient(String secret) throws Exception {
      createClient(secret, true);
    }

    void createClient(String secret, boolean enableAes) throws Exception {
      TransportConf clientConf = enableAes ? conf
        : new TransportConf("rpc", MapConfigProvider.EMPTY);
      List<TransportClientBootstrap> bootstraps = Arrays.asList(
        new AuthClientBootstrap(clientConf, appId, createKeyHolder(secret)));
      this.client = ctx.createClientFactory(bootstraps)
        .createClient(TestUtils.getLocalHost(), server.getPort());
    }

    void close() {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.close();
      }
      if (ctx != null) {
        ctx.close();
      }
    }

    private SecretKeyHolder createKeyHolder(String secret) {
      SecretKeyHolder keyHolder = mock(SecretKeyHolder.class);
      when(keyHolder.getSaslUser(anyString())).thenReturn(appId);
      when(keyHolder.getSecretKey(anyString())).thenReturn(secret);
      return keyHolder;
    }

  }

}
