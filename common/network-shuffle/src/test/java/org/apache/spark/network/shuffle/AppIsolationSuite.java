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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.crypto.AuthServerBootstrap;
import org.apache.spark.network.sasl.SaslClientBootstrap;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class AppIsolationSuite {

  // Use a long timeout to account for slow / overloaded build machines. In the normal case,
  // tests should finish way before the timeout expires.
  private static final long TIMEOUT_MS = 10_000;

  private static SecretKeyHolder secretKeyHolder;
  private static TransportConf conf;

  @BeforeClass
  public static void beforeAll() {
    Map<String, String> confMap = new HashMap<>();
    confMap.put("spark.network.crypto.enabled", "true");
    confMap.put("spark.network.crypto.saslFallback", "false");
    conf = new TransportConf("shuffle", new MapConfigProvider(confMap));

    secretKeyHolder = mock(SecretKeyHolder.class);
    when(secretKeyHolder.getSaslUser(eq("app-1"))).thenReturn("app-1");
    when(secretKeyHolder.getSecretKey(eq("app-1"))).thenReturn("app-1");
    when(secretKeyHolder.getSaslUser(eq("app-2"))).thenReturn("app-2");
    when(secretKeyHolder.getSecretKey(eq("app-2"))).thenReturn("app-2");
  }

  @Test
  public void testSaslAppIsolation() throws Exception {
    testAppIsolation(
      () -> new SaslServerBootstrap(conf, secretKeyHolder),
      appId -> new SaslClientBootstrap(conf, appId, secretKeyHolder));
  }

  @Test
  public void testAuthEngineAppIsolation() throws Exception {
    testAppIsolation(
      () -> new AuthServerBootstrap(conf, secretKeyHolder),
      appId -> new AuthClientBootstrap(conf, appId, secretKeyHolder));
  }

  private void testAppIsolation(
      Supplier<TransportServerBootstrap> serverBootstrap,
      Function<String, TransportClientBootstrap> clientBootstrapFactory) throws Exception {
    // Start a new server with the correct RPC handler to serve block data.
    ExternalShuffleBlockResolver blockResolver = mock(ExternalShuffleBlockResolver.class);
    ExternalShuffleBlockHandler blockHandler = new ExternalShuffleBlockHandler(
      new OneForOneStreamManager(), blockResolver);
    TransportServerBootstrap bootstrap = serverBootstrap.get();
    TransportContext blockServerContext = new TransportContext(conf, blockHandler);

    try (
      TransportServer blockServer = blockServerContext.createServer(Arrays.asList(bootstrap));
      // Create a client, and make a request to fetch blocks from a different app.
      TransportClientFactory clientFactory1 = blockServerContext.createClientFactory(
          Arrays.asList(clientBootstrapFactory.apply("app-1")));
      TransportClient client1 = clientFactory1.createClient(
          TestUtils.getLocalHost(), blockServer.getPort())) {

      AtomicReference<Throwable> exception = new AtomicReference<>();

      CountDownLatch blockFetchLatch = new CountDownLatch(1);
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

      String[] blockIds = { "shuffle_0_1_2", "shuffle_0_3_4" };
      OneForOneBlockFetcher fetcher =
          new OneForOneBlockFetcher(client1, "app-2", "0", blockIds, listener, conf);
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

      try (
        // Create a second client, authenticated with a different app ID, and try to read from
        // the stream created for the previous app.
        TransportClientFactory clientFactory2 = blockServerContext.createClientFactory(
            Arrays.asList(clientBootstrapFactory.apply("app-2")));
        TransportClient client2 = clientFactory2.createClient(
            TestUtils.getLocalHost(), blockServer.getPort())
      ) {
        CountDownLatch chunkReceivedLatch = new CountDownLatch(1);
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
      }
    }
  }

  private static void checkSecurityException(Throwable t) {
    assertNotNull("No exception was caught.", t);
    assertTrue("Expected SecurityException.",
      t.getMessage().contains(SecurityException.class.getName()));
  }
}
