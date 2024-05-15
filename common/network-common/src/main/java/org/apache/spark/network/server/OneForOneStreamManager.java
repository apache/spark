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

package org.apache.spark.network.server;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;, which are
 * individually fetched as chunks by the client. Each registered buffer is one chunk.
 */
public class OneForOneStreamManager extends StreamManager {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);

  private final AtomicLong nextStreamId;
  private final ConcurrentHashMap<Long, StreamState> streams;

  /** State of a single stream. */
  private static class StreamState {
    final String appId;
    final Iterator<ManagedBuffer> buffers;

    // The channel associated to the stream
    final Channel associatedChannel;
    // Indicates whether the buffers is only materialized when next() is called. Some buffers like
    // ShuffleManagedBufferIterator, ShuffleChunkManagedBufferIterator, ManagedBufferIterator are
    // not materialized until the iterator is traversed by calling next(). We use it to decide
    // whether buffers should be released at connectionTerminated() in order to avoid unnecessary
    // buffer materialization, which could be I/O based.
    final boolean isBufferMaterializedOnNext;

    // Used to keep track of the index of the buffer that the user has retrieved, just to ensure
    // that the caller only requests each chunk one at a time, in order.
    int curChunk = 0;

    // Used to keep track of the number of chunks being transferred and not finished yet.
    final AtomicLong chunksBeingTransferred = new AtomicLong(0L);

    StreamState(
        String appId,
        Iterator<ManagedBuffer> buffers,
        Channel channel,
        boolean isBufferMaterializedOnNext) {
      this.appId = appId;
      this.buffers = Preconditions.checkNotNull(buffers);
      this.associatedChannel = channel;
      this.isBufferMaterializedOnNext = isBufferMaterializedOnNext;
    }
  }

  public OneForOneStreamManager() {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<>();
  }

  @Override
  public ManagedBuffer getChunk(long streamId, int chunkIndex) {
    StreamState state = streams.get(streamId);
    if (state == null) {
      throw new IllegalStateException(String.format(
        "Requested chunk not available since streamId %s is closed", streamId));
    } else if (chunkIndex != state.curChunk) {
      throw new IllegalStateException(String.format(
        "Received out-of-order chunk index %s (expected %s)", chunkIndex, state.curChunk));
    } else if (!state.buffers.hasNext()) {
      throw new IllegalStateException(String.format(
        "Requested chunk index beyond end %s", chunkIndex));
    }
    state.curChunk += 1;
    ManagedBuffer nextChunk = state.buffers.next();

    if (!state.buffers.hasNext()) {
      logger.trace("Removing stream id {}", streamId);
      streams.remove(streamId);
    }

    return nextChunk;
  }

  @Override
  public ManagedBuffer openStream(String streamChunkId) {
    Pair<Long, Integer> streamChunkIdPair = parseStreamChunkId(streamChunkId);
    return getChunk(streamChunkIdPair.getLeft(), streamChunkIdPair.getRight());
  }

  public static String genStreamChunkId(long streamId, int chunkId) {
    return String.format("%d_%d", streamId, chunkId);
  }

  // Parse streamChunkId to be stream id and chunk id. This is used when fetch remote chunk as a
  // stream.
  public static Pair<Long, Integer> parseStreamChunkId(String streamChunkId) {
    String[] array = streamChunkId.split("_");
    assert array.length == 2:
      "Stream id and chunk index should be specified.";
    long streamId = Long.valueOf(array[0]);
    int chunkIndex = Integer.valueOf(array[1]);
    return ImmutablePair.of(streamId, chunkIndex);
  }

  @Override
  public void connectionTerminated(Channel channel) {
    RuntimeException failedToReleaseBufferException = null;

    // Close all streams which have been associated with the channel.
    for (Map.Entry<Long, StreamState> entry: streams.entrySet()) {
      StreamState state = entry.getValue();
      if (state.associatedChannel == channel) {
        streams.remove(entry.getKey());

        try {
          // Release all remaining buffers.
          while (!state.isBufferMaterializedOnNext && state.buffers.hasNext()) {
            ManagedBuffer buffer = state.buffers.next();
            if (buffer != null) {
              buffer.release();
            }
          }
        } catch (RuntimeException e) {
          if (failedToReleaseBufferException == null) {
            failedToReleaseBufferException = e;
          } else {
            logger.error("Exception trying to release remaining StreamState buffers", e);
          }
        }
      }
    }

    if (failedToReleaseBufferException != null) {
      throw failedToReleaseBufferException;
    }
  }

  @Override
  public void checkAuthorization(TransportClient client, long streamId) {
    if (client.getClientId() != null) {
      StreamState state = streams.get(streamId);
      Preconditions.checkArgument(state != null, "Unknown stream ID.");
      if (!client.getClientId().equals(state.appId)) {
        throw new SecurityException(String.format(
          "Client %s not authorized to read stream %d (app %s).",
          client.getClientId(),
          streamId,
          state.appId));
      }
    }
  }

  @Override
  public void chunkBeingSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred.incrementAndGet();
    }

  }

  @Override
  public void streamBeingSent(String streamId) {
    chunkBeingSent(parseStreamChunkId(streamId).getLeft());
  }

  @Override
  public void chunkSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred.decrementAndGet();
    }
  }

  @Override
  public void streamSent(String streamId) {
    chunkSent(OneForOneStreamManager.parseStreamChunkId(streamId).getLeft());
  }

  @Override
  public long chunksBeingTransferred() {
    long sum = 0L;
    for (StreamState streamState: streams.values()) {
      sum += streamState.chunksBeingTransferred.get();
    }
    return sum;
  }

  /**
   * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
   * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
   * client connection is closed before the iterator is fully drained, then the remaining
   * materialized buffers will all be release()'d, but some buffers like
   * ShuffleManagedBufferIterator, ShuffleChunkManagedBufferIterator, ManagedBufferIterator should
   * not release, because they have not been materialized before requesting the iterator by
   * the next method.
   *
   * If an app ID is provided, only callers who've authenticated with the given app ID will be
   * allowed to fetch from this stream.
   *
   * This method also associates the stream with a single client connection, which is guaranteed
   * to be the only reader of the stream. Once the connection is closed, the stream will never
   * be used again, enabling cleanup by `connectionTerminated`.
   */
  public long registerStream(
      String appId,
      Iterator<ManagedBuffer> buffers,
      Channel channel,
      boolean isBufferMaterializedOnNext) {
    long myStreamId = nextStreamId.getAndIncrement();
    streams.put(myStreamId, new StreamState(appId, buffers, channel, isBufferMaterializedOnNext));
    return myStreamId;
  }

  public long registerStream(String appId, Iterator<ManagedBuffer> buffers, Channel channel) {
    return registerStream(appId, buffers, channel, false);
  }

  @VisibleForTesting
  public int numStreamStates() {
    return streams.size();
  }
}
