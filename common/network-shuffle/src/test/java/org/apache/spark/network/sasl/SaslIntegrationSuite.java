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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver;
import org.apache.spark.network.shuffle.OneForOneBlockFetcher;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
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
    conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
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
      Lists.<TransportClientBootstrap>newArrayList(
        new SaslClientBootstrap(conf, "app-1", secretKeyHolder)));

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
      Lists.<TransportClientBootstrap>newArrayList(
        new SaslClientBootstrap(conf, "unknown-app", badKeyHolder)));

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
    clientFactory = context.createClientFactory(
      Lists.<TransportClientBootstrap>newArrayList());

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
      Lists.<TransportClientBootstrap>newArrayList(
        new SaslClientBootstrap(conf, "app-1", secretKeyHolder)));
    TransportServer server = context.createServer();
    try {
      clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Digest-challenge format violation"));
    } finally {
      server.close();
    }
  }

  /**
   * This test is not actually testing SASL behavior, but testing that the shuffle service
   * performs correct authorization checks based on the SASL authentication data.
   */
  @Test
  public void testAppIsolation() throws Exception {
    // Start a new server with the correct RPC handler to serve block data.
    ExternalShuffleBlockResolver blockResolver = mock(ExternalShuffleBlockResolver.class);
    ExternalShuffleBlockHandler blockHandler = new ExternalShuffleBlockHandler(
      new OneForOneStreamManager(), blockResolver);
    TransportServerBootstrap bootstrap = new SaslServerBootstrap(conf, secretKeyHolder);
    TransportContext blockServerContext = new TransportContext(conf, blockHandler);
    TransportServer blockServer = blockServerContext.createServer(Arrays.asList(bootstrap));

    TransportClient client1 = null;
    TransportClient client2 = null;
    TransportClientFactory clientFactory2 = null;
    try {
      // Create a client, and make a request to fetch blocks from a different app.
      clientFactory = blockServerContext.createClientFactory(
        Lists.<TransportClientBootstrap>newArrayList(
          new SaslClientBootstrap(conf, "app-1", secretKeyHolder)));
      client1 = clientFactory.createClient(TestUtils.getLocalHost(),
        blockServer.getPort());

      final AtomicReference<Throwable> exception = new AtomicReference<>();

      final CountDownLatch blockFetchLatch = new CountDownLatch(1);
      BlockFetchingListener listener = new BlockFetchingListener() {
        @Override
        public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
          blockFetchLatch.countDown();
        }
        @Override
        public void onBlockFetchFailure(String blockId, Throwable t) {
          exception.set(t);
          blockFetchLatch.countDown();
        }
      };

      String[] blockIds = { "shuffle_2_3_4", "shuffle_6_7_8" };
      OneForOneBlockFetcher fetcher =
          new OneForOneBlockFetcher(client1, "app-2", "0", blockIds, listener);
      fetcher.start();
      blockFetchLatch.await();
      checkSecurityException(exception.get());

      // Register an executor so that the next steps work.
      ExecutorShuffleInfo executorInfo = new ExecutorShuffleInfo(
        new String[] { System.getProperty("java.io.tmpdir") }, 1,
          "org.apache.spark.shuffle.sort.SortShuffleManager");
      RegisterExecutor regmsg = new RegisterExecutor("app-1", "0", executorInfo);
      client1.sendRpcSync(regmsg.toByteBuffer(), TIMEOUT_MS);

      // Make a successful request to fetch blocks, which creates a new stream. But do not actually
      // fetch any blocks, to keep the stream open.
      OpenBlocks openMessage = new OpenBlocks("app-1", "0", blockIds);
      ByteBuffer response = client1.sendRpcSync(openMessage.toByteBuffer(), TIMEOUT_MS);
      StreamHandle stream = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
      long streamId = stream.streamId;

      // Create a second client, authenticated with a different app ID, and try to read from
      // the stream created for the previous app.
      clientFactory2 = blockServerContext.createClientFactory(
        Lists.<TransportClientBootstrap>newArrayList(
          new SaslClientBootstrap(conf, "app-2", secretKeyHolder)));
      client2 = clientFactory2.createClient(TestUtils.getLocalHost(),
        blockServer.getPort());

      final CountDownLatch chunkReceivedLatch = new CountDownLatch(1);
      ChunkReceivedCallback callback = new ChunkReceivedCallback() {
        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
          chunkReceivedLatch.countDown();
        }
        @Override
        public void onFailure(int chunkIndex, Throwable t) {
          exception.set(t);
          chunkReceivedLatch.countDown();
        }
      };

      exception.set(null);
      client2.fetchChunk(streamId, 0, callback);
      chunkReceivedLatch.await();
      checkSecurityException(exception.get());
    } finally {
      if (client1 != null) {
        client1.close();
      }
      if (client2 != null) {
        client2.close();
      }
      if (clientFactory2 != null) {
        clientFactory2.close();
      }
      blockServer.close();
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

  private void checkSecurityException(Throwable t) {
    assertNotNull("No exception was caught.", t);
    assertTrue("Expected SecurityException.",
      t.getMessage().contains(SecurityException.class.getName()));
  }
}
