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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.sasl.SaslException;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.junit.jupiter.api.Test;

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
import org.apache.spark.network.util.*;

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
  public void testSaslAuthentication() throws Throwable {
    testBasicSasl(false);
  }

  @Test
  public void testSaslEncryption() throws Throwable {
    testBasicSasl(true);
  }

  private static void testBasicSasl(boolean encrypt) throws Throwable {
    RpcHandler rpcHandler = mock(RpcHandler.class);
    doAnswer(invocation -> {
      ByteBuffer message = (ByteBuffer) invocation.getArguments()[1];
      RpcResponseCallback cb = (RpcResponseCallback) invocation.getArguments()[2];
      assertEquals("Ping", JavaUtils.bytesToString(message));
      cb.onSuccess(JavaUtils.stringToBytes("Pong"));
      return null;
    })
      .when(rpcHandler)
      .receive(any(TransportClient.class), any(ByteBuffer.class), any(RpcResponseCallback.class));

    try (SaslTestCtx ctx = new SaslTestCtx(rpcHandler, encrypt, false)) {
      ByteBuffer response = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"),
        TimeUnit.SECONDS.toMillis(10));
      assertEquals("Pong", JavaUtils.bytesToString(response));
    } finally {
      // There should be 2 terminated events; one for the client, one for the server.
      Throwable error = null;
      long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (deadline > System.nanoTime()) {
        try {
          verify(rpcHandler, times(2)).channelInactive(any(TransportClient.class));
          error = null;
          break;
        } catch (Throwable t) {
          error = t;
          TimeUnit.MILLISECONDS.sleep(10);
        }
      }
      if (error != null) {
        throw error;
      }
    }
  }

  @Test
  public void testEncryptedMessage() throws Exception {
    SaslEncryptionBackend backend = mock(SaslEncryptionBackend.class);
    byte[] data = new byte[1024];
    new Random().nextBytes(data);
    when(backend.wrap(any(byte[].class), anyInt(), anyInt())).thenReturn(data);

    ByteBuf msg = Unpooled.buffer();
    try {
      msg.writeBytes(data);

      // Create a channel with a really small buffer compared to the data. This means that on each
      // call, the outbound data will not be fully written, so the write() method should return a
      // dummy count to keep the channel alive when possible.
      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(32);

      SaslEncryption.EncryptedMessage emsg =
        new SaslEncryption.EncryptedMessage(backend, msg, 1024);
      long count = emsg.transferTo(channel, emsg.transferred());
      assertTrue(count < data.length);
      assertTrue(count > 0);

      // Here, the output buffer is full so nothing should be transferred.
      assertEquals(0, emsg.transferTo(channel, emsg.transferred()));

      // Now there's room in the buffer, but not enough to transfer all the remaining data,
      // so the dummy count should be returned.
      channel.reset();
      assertEquals(1, emsg.transferTo(channel, emsg.transferred()));

      // Eventually, the whole message should be transferred.
      for (int i = 0; i < data.length / 32 - 2; i++) {
        channel.reset();
        assertEquals(1, emsg.transferTo(channel, emsg.transferred()));
      }

      channel.reset();
      count = emsg.transferTo(channel, emsg.transferred());
      assertTrue(count > 1 && count < data.length, "Unexpected count: " + count);
      assertEquals(data.length, emsg.transferred());
    } finally {
      msg.release();
    }
  }

  @Test
  public void testEncryptedMessageChunking() throws Exception {
    File file = File.createTempFile("sasltest", ".txt");
    try {
      TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);

      byte[] data = new byte[8 * 1024];
      new Random().nextBytes(data);
      Files.write(data, file);

      SaslEncryptionBackend backend = mock(SaslEncryptionBackend.class);
      // It doesn't really matter what we return here, as long as it's not null.
      when(backend.wrap(any(byte[].class), anyInt(), anyInt())).thenReturn(data);

      FileSegmentManagedBuffer msg = new FileSegmentManagedBuffer(conf, file, 0, file.length());
      SaslEncryption.EncryptedMessage emsg =
        new SaslEncryption.EncryptedMessage(backend, msg.convertToNetty(), data.length / 8);

      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(data.length);
      while (emsg.transferred() < emsg.count()) {
        channel.reset();
        emsg.transferTo(channel, emsg.transferred());
      }

      verify(backend, times(8)).wrap(any(byte[].class), anyInt(), anyInt());
    } finally {
      file.delete();
    }
  }

  @Test
  public void testFileRegionEncryption() throws Exception {
    Map<String, String> testConf = ImmutableMap.of(
      "spark.network.sasl.maxEncryptedBlockSize", "1k");

    AtomicReference<ManagedBuffer> response = new AtomicReference<>();
    File file = File.createTempFile("sasltest", ".txt");
    SaslTestCtx ctx = null;
    try {
      TransportConf conf = new TransportConf("shuffle", new MapConfigProvider(testConf));
      StreamManager sm = mock(StreamManager.class);
      when(sm.getChunk(anyLong(), anyInt())).thenAnswer(invocation ->
          new FileSegmentManagedBuffer(conf, file, 0, file.length()));

      RpcHandler rpcHandler = mock(RpcHandler.class);
      when(rpcHandler.getStreamManager()).thenReturn(sm);

      byte[] data = new byte[8 * 1024];
      new Random().nextBytes(data);
      Files.write(data, file);

      ctx = new SaslTestCtx(rpcHandler, true, false, testConf);

      CountDownLatch lock = new CountDownLatch(1);

      ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
      doAnswer(invocation -> {
        response.set((ManagedBuffer) invocation.getArguments()[1]);
        response.get().retain();
        lock.countDown();
        return null;
      }).when(callback).onSuccess(anyInt(), any(ManagedBuffer.class));

      ctx.client.fetchChunk(0, 0, callback);
      lock.await(10, TimeUnit.SECONDS);

      verify(callback, times(1)).onSuccess(anyInt(), any(ManagedBuffer.class));
      verify(callback, never()).onFailure(anyInt(), any(Throwable.class));

      byte[] received = ByteStreams.toByteArray(response.get().createInputStream());
      assertArrayEquals(data, received);
    } finally {
      file.delete();
      if (ctx != null) {
        ctx.close();
      }
      if (response.get() != null) {
        response.get().release();
      }
    }
  }

  @Test
  public void testServerAlwaysEncrypt() {
    Exception re = assertThrows(Exception.class,
      () -> new SaslTestCtx(mock(RpcHandler.class), false, false,
              ImmutableMap.of("spark.network.sasl.serverAlwaysEncrypt", "true")));
    assertTrue(re.getCause() instanceof SaslException);
  }

  @Test
  public void testDataEncryptionIsActuallyEnabled() throws Exception {
    // This test sets up an encrypted connection but then, using a client bootstrap, removes
    // the encryption handler from the client side. This should cause the server to not be
    // able to understand RPCs sent to it and thus close the connection.
    try (SaslTestCtx ctx = new SaslTestCtx(mock(RpcHandler.class), true, true)) {
      Exception e = assertThrows(Exception.class,
        () -> ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"),
                TimeUnit.SECONDS.toMillis(10)));
      assertFalse(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void testRpcHandlerDelegate() throws Exception {
    // Tests all delegates exception for receive(), which is more complicated and already handled
    // by all other tests.
    RpcHandler handler = mock(RpcHandler.class);
    RpcHandler saslHandler = new SaslRpcHandler(null, null, handler, null);

    saslHandler.getStreamManager();
    verify(handler).getStreamManager();

    saslHandler.channelInactive(null);
    verify(handler).channelInactive(isNull());

    saslHandler.exceptionCaught(null, null);
    verify(handler).exceptionCaught(isNull(), isNull());
  }

  @Test
  public void testDelegates() throws Exception {
    Method[] rpcHandlerMethods = RpcHandler.class.getDeclaredMethods();
    for (Method m : rpcHandlerMethods) {
      Method delegate = SaslRpcHandler.class.getMethod(m.getName(), m.getParameterTypes());
      assertNotEquals(delegate.getDeclaringClass(), RpcHandler.class);
    }
  }

  private static class SaslTestCtx implements AutoCloseable {

    final TransportClient client;
    final TransportServer server;
    final TransportContext ctx;

    private final boolean encrypt;
    private final boolean disableClientEncryption;
    private final EncryptionCheckerBootstrap checker;

    SaslTestCtx(
        RpcHandler rpcHandler,
        boolean encrypt,
        boolean disableClientEncryption)
      throws Exception {

      this(rpcHandler, encrypt, disableClientEncryption, Collections.emptyMap());
    }

    SaslTestCtx(
        RpcHandler rpcHandler,
        boolean encrypt,
        boolean disableClientEncryption,
        Map<String, String> extraConf)
      throws Exception {

      Map<String, String> testConf = ImmutableMap.<String, String>builder()
        .putAll(extraConf)
        .put("spark.authenticate.enableSaslEncryption", String.valueOf(encrypt))
        .build();
      TransportConf conf = new TransportConf("shuffle", new MapConfigProvider(testConf));

      SecretKeyHolder keyHolder = mock(SecretKeyHolder.class);
      when(keyHolder.getSaslUser(anyString())).thenReturn("user");
      when(keyHolder.getSecretKey(anyString())).thenReturn("secret");

      this.ctx = new TransportContext(conf, rpcHandler);

      this.checker = new EncryptionCheckerBootstrap(SaslEncryption.ENCRYPTION_HANDLER_NAME);

      this.server = ctx.createServer(Arrays.asList(new SaslServerBootstrap(conf, keyHolder),
        checker));

      try {
        List<TransportClientBootstrap> clientBootstraps = new ArrayList<>();
        clientBootstraps.add(new SaslClientBootstrap(conf, "user", keyHolder));
        if (disableClientEncryption) {
          clientBootstraps.add(new EncryptionDisablerBootstrap());
        }

        this.client = ctx.createClientFactory(clientBootstraps)
          .createClient(TestUtils.getLocalHost(), server.getPort());
      } catch (Exception e) {
        close();
        throw e;
      }

      this.encrypt = encrypt;
      this.disableClientEncryption = disableClientEncryption;
    }

    public void close() {
      if (!disableClientEncryption) {
        assertEquals(encrypt, checker.foundEncryptionHandler);
      }
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

  }

  private static class EncryptionCheckerBootstrap extends ChannelOutboundHandlerAdapter
    implements TransportServerBootstrap {

    boolean foundEncryptionHandler;
    String encryptHandlerName;

    EncryptionCheckerBootstrap(String encryptHandlerName) {
      this.encryptHandlerName = encryptHandlerName;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
      if (!foundEncryptionHandler) {
        foundEncryptionHandler =
          ctx.channel().pipeline().get(encryptHandlerName) != null;
      }
      ctx.write(msg, promise);
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
