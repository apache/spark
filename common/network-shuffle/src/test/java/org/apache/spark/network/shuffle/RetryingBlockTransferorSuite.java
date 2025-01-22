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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.sasl.SaslTimeoutException;
import static org.apache.spark.network.shuffle.RetryingBlockTransferor.BlockTransferStarter;

/**
 * Tests retry logic by throwing IOExceptions and ensuring that subsequent attempts are made to
 * fetch the lost blocks.
 */
public class RetryingBlockTransferorSuite {

  private final ManagedBuffer block0 = new NioManagedBuffer(ByteBuffer.wrap(new byte[13]));
  private final ManagedBuffer block1 = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
  private final ManagedBuffer block2 = new NioManagedBuffer(ByteBuffer.wrap(new byte[19]));
  private static Map<String, String> configMap;
  private static RetryingBlockTransferor _retryingBlockTransferor;

  private static final int MAX_RETRIES = 2;

  @BeforeEach
  public void initMap() {
    configMap = new HashMap<String, String>() {{
      put("spark.shuffle.io.maxRetries", Integer.toString(MAX_RETRIES));
      put("spark.shuffle.io.retryWait", "0");
    }};
  }

  @Test
  public void testNoFailures() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // Immediately return both blocks successfully.
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", block1)
        .build()
      );

    performInteractions(interactions, listener);

    verify(listener).onBlockTransferSuccess("b0", block0);
    verify(listener).onBlockTransferSuccess("b1", block1);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testUnrecoverableFailure() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0 throws a non-IOException error, so it will be failed without retry.
      ImmutableMap.<String, Object>builder()
        .put("b0", new RuntimeException("Ouch!"))
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener).onBlockTransferFailure(eq("b0"), any());
    verify(listener).onBlockTransferSuccess("b1", block1);
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testSingleIOExceptionOnFirst() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // IOException will cause a retry. Since b0 fails, we will retry both.
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException("Connection failed or something"))
        .put("b1", block1)
        .build(),
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockTransferSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockTransferSuccess("b1", block1);
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testSingleIOExceptionOnSecond() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // IOException will cause a retry. Since b1 fails, we will not retry b0.
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new IOException("Connection failed or something"))
        .build(),
      ImmutableMap.<String, Object>builder()
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockTransferSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockTransferSuccess("b1", block1);
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testTwoIOExceptions() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0's IOException will trigger retry, b1's will be ignored.
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException())
        .put("b1", new IOException())
        .build(),
      // Next, b0 is successful and b1 errors again, so we just request that one.
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new IOException())
        .build(),
      // b1 returns successfully within 2 retries.
      ImmutableMap.<String, Object>builder()
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockTransferSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockTransferSuccess("b1", block1);
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testThreeIOExceptions() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0's IOException will trigger retry, b1's will be ignored.
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException())
        .put("b1", new IOException())
        .build(),
      // Next, b0 is successful and b1 errors again, so we just request that one.
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new IOException())
        .build(),
      // b1 errors again, but this was the last retry
      ImmutableMap.<String, Object>builder()
        .put("b1", new IOException())
        .build(),
      // This is not reached -- b1 has failed.
      ImmutableMap.<String, Object>builder()
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockTransferSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockTransferFailure(eq("b1"), any());
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testRetryAndUnrecoverable() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0's IOException will trigger retry, subsequent messages will be ignored.
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException())
        .put("b1", new RuntimeException())
        .put("b2", block2)
        .build(),
      // Next, b0 is successful, b1 errors unrecoverably, and b2 triggers a retry.
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new RuntimeException())
        .put("b2", new IOException())
        .build(),
      // b2 succeeds in its last retry.
      ImmutableMap.<String, Object>builder()
        .put("b2", block2)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockTransferSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockTransferFailure(eq("b1"), any());
    verify(listener, timeout(5000)).onBlockTransferSuccess("b2", block2);
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testSaslTimeoutFailure() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);
    TimeoutException timeoutException = new TimeoutException();
    SaslTimeoutException saslTimeoutException =
        new SaslTimeoutException(timeoutException);
    List<? extends Map<String, Object>> interactions = Arrays.asList(
        ImmutableMap.<String, Object>builder()
            .put("b0", saslTimeoutException)
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("b0", block0)
            .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockTransferFailure("b0", saslTimeoutException);
    verify(listener).getTransferType();
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testRetryOnSaslTimeout() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
        // SaslTimeout will cause a retry. Since b0 fails, we will retry both.
        ImmutableMap.<String, Object>builder()
            .put("b0", new SaslTimeoutException(new TimeoutException()))
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("b0", block0)
            .build()
    );
    configMap.put("spark.shuffle.sasl.enableRetries", "true");
    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockTransferSuccess("b0", block0);
    verify(listener).getTransferType();
    verifyNoMoreInteractions(listener);
    assertEquals(0, _retryingBlockTransferor.getRetryCount());
  }

  @Test
  public void testRepeatedSaslRetryFailures() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);
    TimeoutException timeoutException = new TimeoutException();
    SaslTimeoutException saslTimeoutException =
        new SaslTimeoutException(timeoutException);
    List<ImmutableMap<String, Object>> interactions = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      interactions.add(
          ImmutableMap.<String, Object>builder()
              .put("b0", saslTimeoutException)
              .build()
      );
    }
    configMap.put("spark.shuffle.sasl.enableRetries", "true");
    performInteractions(interactions, listener);
    verify(listener, timeout(5000)).onBlockTransferFailure("b0", saslTimeoutException);
    verify(listener, times(3)).getTransferType();
    verifyNoMoreInteractions(listener);
    assertEquals(MAX_RETRIES, _retryingBlockTransferor.getRetryCount());
  }

  @Test
  public void testBlockTransferFailureAfterSasl() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
        ImmutableMap.<String, Object>builder()
            .put("b0", new SaslTimeoutException(new TimeoutException()))
            .put("b1", new IOException())
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("b0", block0)
            .put("b1", new IOException())
            .build(),
        ImmutableMap.<String, Object>builder()
          .put("b1", block1)
          .build()
    );
    configMap.put("spark.shuffle.sasl.enableRetries", "true");
    performInteractions(interactions, listener);
    verify(listener, timeout(5000)).onBlockTransferSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockTransferSuccess("b1", block1);
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
    // This should be equal to 1 because after the SASL exception is retried,
    // retryCount should be set back to 0. Then after that b1 encounters an
    // exception that is retried.
    assertEquals(1, _retryingBlockTransferor.getRetryCount());
  }

  @Test
  public void testIOExceptionFailsConnectionEvenWithSaslException()
    throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    SaslTimeoutException saslExceptionInitial = new SaslTimeoutException("initial",
            new TimeoutException());
    SaslTimeoutException saslExceptionFinal = new SaslTimeoutException("final",
            new TimeoutException());
    IOException ioException = new IOException();
    List<? extends Map<String, Object>> interactions = Arrays.asList(
            ImmutableMap.of("b0", saslExceptionInitial),
            ImmutableMap.of("b0", ioException),
            ImmutableMap.of("b0", saslExceptionInitial),
            ImmutableMap.of("b0", ioException),
            ImmutableMap.of("b0", saslExceptionFinal),
            // will not get invoked because the connection fails
            ImmutableMap.of("b0", ioException),
            // will not get invoked
            ImmutableMap.of("b0", block0)
    );
    configMap.put("spark.shuffle.sasl.enableRetries", "true");
    performInteractions(interactions, listener);
    verify(listener, timeout(5000)).onBlockTransferFailure("b0", saslExceptionFinal);
    verify(listener, atLeastOnce()).getTransferType();
    verifyNoMoreInteractions(listener);
    assertEquals(MAX_RETRIES, _retryingBlockTransferor.getRetryCount());
  }

  @Test
  public void testRetryInitiationFailure() throws IOException, InterruptedException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
        // IOException will initiate a retry, but the initiation will fail
        ImmutableMap.<String, Object>builder()
            .put("b0", new IOException("Connection failed or something"))
            .put("b1", block1)
            .build()
    );

    configureInteractions(interactions, listener);
    _retryingBlockTransferor = spy(_retryingBlockTransferor);
    // Simulate a failure to initiate a retry.
    doReturn(false).when(_retryingBlockTransferor).initiateRetry(any());
    // Override listener, so that it delegates to the spied instance and not the original class.
    _retryingBlockTransferor.setCurrentListener(
        _retryingBlockTransferor.new RetryingBlockTransferListener());
    _retryingBlockTransferor.start();

    verify(listener, timeout(5000)).onBlockTransferFailure(eq("b0"), any());
    verify(listener, timeout(5000)).onBlockTransferSuccess("b1", block1);
    verifyNoMoreInteractions(listener);
  }

  /**
   * Performs a set of interactions in response to block requests from a RetryingBlockFetcher.
   * Each interaction is a Map from BlockId to either ManagedBuffer or Exception. This interaction
   * means "respond to the next block fetch request with these Successful buffers and these Failure
   * exceptions". We verify that the expected block ids are exactly the ones requested.
   *
   * If multiple interactions are supplied, they will be used in order. This is useful for encoding
   * retries -- the first interaction may include an IOException, which causes a retry of some
   * subset of the original blocks in a second interaction.
   */
  private static void performInteractions(List<? extends Map<String, Object>> interactions,
                                          BlockFetchingListener listener)
      throws IOException, InterruptedException {
    configureInteractions(interactions, listener);
    _retryingBlockTransferor.start();
  }

  private static void configureInteractions(List<? extends Map<String, Object>> interactions,
                                          BlockFetchingListener listener)
    throws IOException, InterruptedException {

    MapConfigProvider provider = new MapConfigProvider(configMap);
    TransportConf conf = new TransportConf("shuffle", provider);
    BlockTransferStarter fetchStarter = mock(BlockTransferStarter.class);

    Stubber stub = null;

    // Contains all blockIds that are referenced across all interactions.
    LinkedHashSet<String> blockIds = Sets.newLinkedHashSet();

    for (Map<String, Object> interaction : interactions) {
      blockIds.addAll(interaction.keySet());

      Answer<Void> answer = invocationOnMock -> {
        try {
          // Verify that the RetryingBlockFetcher requested the expected blocks.
          String[] requestedBlockIds = (String[]) invocationOnMock.getArguments()[0];
          String[] desiredBlockIds = interaction.keySet().toArray(new String[interaction.size()]);
          assertArrayEquals(desiredBlockIds, requestedBlockIds);

          // Now actually invoke the success/failure callbacks on each block.
          BlockFetchingListener retryListener =
            (BlockFetchingListener) invocationOnMock.getArguments()[1];
          for (Map.Entry<String, Object> block : interaction.entrySet()) {
            String blockId = block.getKey();
            Object blockValue = block.getValue();

            if (blockValue instanceof ManagedBuffer managedBuffer) {
              retryListener.onBlockFetchSuccess(blockId, managedBuffer);
            } else if (blockValue instanceof Exception exception) {
              retryListener.onBlockFetchFailure(blockId, exception);
            } else {
              fail("Can only handle ManagedBuffers and Exceptions, got " + blockValue);
            }
          }
          return null;
        } catch (Throwable e) {
          e.printStackTrace();
          throw e;
        }
      };

      // This is either the first stub, or should be chained behind the prior ones.
      if (stub == null) {
        stub = doAnswer(answer);
      } else {
        stub.doAnswer(answer);
      }
    }

    assertNotNull(stub);
    stub.when(fetchStarter).createAndStart(any(), any());
    String[] blockIdArray = blockIds.toArray(new String[blockIds.size()]);
    _retryingBlockTransferor =
        new RetryingBlockTransferor(conf, fetchStarter, blockIdArray, listener);
  }
}
