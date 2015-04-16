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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.sasl.SaslException;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
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
    SparkSaslServer server = new SparkSaslServer("shared-secret", secretKeyHolder, false);

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
    SparkSaslServer server = new SparkSaslServer("your-secret", secretKeyHolder, false);

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
    testBasicSasl(false);
  }

  @Test
  public void testSaslEncryption() throws Exception {
    testBasicSasl(true);
  }

  @Test
  public void testFileRegionEncryption() throws Exception {
    final String blockSizeConf = "spark.network.sasl.maxEncryptedBlockSizeKb";
    System.setProperty(blockSizeConf, "1");

    final AtomicReference<ManagedBuffer> response = new AtomicReference();
    final File file = File.createTempFile("sasltest", ".txt");
    SaslTestCtx ctx = null;
    try {
      final TransportConf conf = new TransportConf(new SystemPropertyConfigProvider());
      StreamManager sm = mock(StreamManager.class);
      when(sm.getChunk(anyLong(), anyInt())).thenAnswer(new Answer<ManagedBuffer>() {
          @Override
          public ManagedBuffer answer(InvocationOnMock invocation) {
            return new FileSegmentManagedBuffer(conf, file, 0, file.length());
          }
        });

      RpcHandler rpcHandler = mock(RpcHandler.class);
      when(rpcHandler.getStreamManager()).thenReturn(sm);

      byte[] data = new byte[8 * 1024];
      new Random().nextBytes(data);
      Files.write(data, file);

      ctx = new SaslTestCtx(rpcHandler, true, Arrays.asList(new EncryptionCheckerBootstrap()),
        null);

      final Object lock = new Object();

      ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
      doAnswer(new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) {
            response.set((ManagedBuffer) invocation.getArguments()[1]);
            response.get().retain();
            synchronized (lock) {
              lock.notifyAll();
            }
            return null;
          }
        }).when(callback).onSuccess(anyInt(), any(ManagedBuffer.class));

      synchronized (lock) {
        ctx.client.fetchChunk(0, 0, callback);
        lock.wait(10 * 1000);
      }

      verify(callback, times(1)).onSuccess(anyInt(), any(ManagedBuffer.class));
      verify(callback, never()).onFailure(anyInt(), any(Throwable.class));

      byte[] received = ByteStreams.toByteArray(response.get().createInputStream());
      assertTrue(Arrays.equals(data, received));
    } finally {
      file.delete();
      if (ctx != null) {
        ctx.close();
      }
      if (response.get() != null) {
        response.get().release();
      }
      System.clearProperty(blockSizeConf);
    }
  }

  @Test
  public void testServerAlwaysEncrypt() throws Exception {
    final String alwaysEncryptConfName = "spark.network.sasl.serverAlwaysEncrypt";
    System.setProperty(alwaysEncryptConfName, "true");

    SaslTestCtx ctx = null;
    try {
      ctx = new SaslTestCtx(mock(RpcHandler.class), false, null, null);
      fail("Should have failed to connect without encryption.");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof SaslException);
    } finally {
      if (ctx != null) {
        ctx.close();
      }
      System.clearProperty(alwaysEncryptConfName);
    }
  }

  @Test
  public void testDataEncryptionIsActuallyEnabled() throws Exception {
    // This test sets up an encrypted connection but then, using a client bootstrap, removes
    // the encryption handler from the client side. This should cause the server to not be
    // able to understand RPCs sent to it and thus close the connection.
    SaslTestCtx ctx = null;
    try {
      ctx = new SaslTestCtx(mock(RpcHandler.class), true, null,
        Arrays.asList(new EncryptionDisablerBootstrap()));
      ctx.client.sendRpcSync("Ping".getBytes(UTF_8), TimeUnit.SECONDS.toMillis(10));
      fail("Should have failed to send RPC to server.");
    } catch (Exception e) {
      assertFalse(e.getCause() instanceof TimeoutException);
    } finally {
      if (ctx != null) {
        ctx.close();
      }
    }
  }

  private void testBasicSasl(boolean encrypt) throws Exception {
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

    SaslTestCtx ctx = new SaslTestCtx(rpcHandler, encrypt,
        Arrays.asList(new EncryptionCheckerBootstrap()), null);
    try {
      byte[] response = ctx.client.sendRpcSync("Ping".getBytes(UTF_8), TimeUnit.SECONDS.toMillis(10));
      assertEquals("Pong", new String(response, UTF_8));
    } finally {
      ctx.close();
    }
  }

  private static class SaslTestCtx {

    final TransportClient client;
    final TransportServer server;

    private final boolean encrypt;

    SaslTestCtx(
        RpcHandler rpcHandler,
        boolean encrypt,
        List<? extends TransportServerBootstrap> serverBootstraps,
        List<? extends TransportClientBootstrap> clientBootstraps)
      throws Exception {

      TransportConf conf = new TransportConf(new SystemPropertyConfigProvider());

      SecretKeyHolder keyHolder = mock(SecretKeyHolder.class);
      when(keyHolder.getSaslUser(anyString())).thenReturn("user");
      when(keyHolder.getSecretKey(anyString())).thenReturn("secret");

      TransportContext ctx = new TransportContext(conf, rpcHandler);

      List<TransportServerBootstrap> allServerBootstraps = Lists.newArrayList();
      TransportServerBootstrap saslServerBootstrap = spy(new SaslServerBootstrap(conf, keyHolder));
      allServerBootstraps.add(saslServerBootstrap);
      if (serverBootstraps != null) {
        allServerBootstraps.addAll(serverBootstraps);
      }
      this.server = ctx.createServer(allServerBootstraps);

      TransportClient client = null;
      try {
        List<TransportClientBootstrap> allClientBootstraps = Lists.newArrayList();
        allClientBootstraps.add(new SaslClientBootstrap(conf, "user", keyHolder, encrypt));
        if (clientBootstraps != null) {
          allClientBootstraps.addAll(clientBootstraps);
        }

        this.client = ctx.createClientFactory(allClientBootstraps)
          .createClient(TestUtils.getLocalHost(), server.getPort());

        verify(saslServerBootstrap, times(1))
          .doBootstrap(any(Channel.class), any(RpcHandler.class));
      } catch (Exception e) {
        close();
        throw e;
      }

      this.encrypt = encrypt;

    }

    void close() {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.close();
      }
    }

  }

  private static class EncryptionCheckerBootstrap extends ChannelOutboundHandlerAdapter
    implements TransportServerBootstrap {

    boolean foundEncryptionHandler;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
      if (!foundEncryptionHandler) {
        foundEncryptionHandler =
          ctx.channel().pipeline().get(SaslEncryption.ENCRYPTION_HANDLER_NAME) != null;
      }
      ctx.write(msg, promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      ctx.close(promise);
      assertTrue(foundEncryptionHandler);
    }

    @Override
    public RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler) {
      channel.pipeline().addFirst("encryptionChecker", this);
      return rpcHandler;
    }

  }

  private static class EncryptionDisablerBootstrap implements TransportClientBootstrap {

    @Override
    public void doBootstrap(TransportClient client, Channel channel) {
      channel.pipeline().remove(SaslEncryption.ENCRYPTION_HANDLER_NAME);
    }

  }

}
