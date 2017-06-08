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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;, which are
 * individually fetched as chunks by the client. Each registered buffer is one chunk.
 */
public class OneForOneStreamManager extends StreamManager {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);

  private final ChunkGetter chunkGetter;
  private final AtomicLong nextStreamId;
  private final ConcurrentHashMap<Long, StreamState> streamStates;

  /** State of a single stream. */
  private static class StreamState {
    final String appId;
    final String executorId;

    // The channel associated to the stream
    Channel associatedChannel = null;

    // Used to keep track of the index of the buffer that the user has retrieved, just to ensure
    // that the caller only requests each chunk one at a time, in order.
    int curChunk = 0;

    StreamState(String appId, String executorId) {
      this.appId = appId;
      this.executorId = executorId;
    }
  }

  public interface ChunkGetter {
    ManagedBuffer getChunk(String appId, String executorId, String chunkId);
  }

  public OneForOneStreamManager() {
    this(null);
  }

  public OneForOneStreamManager(ChunkGetter chunkGetter) {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streamStates = new ConcurrentHashMap<>();
    this.chunkGetter = chunkGetter;
  }

  @Override
  public void registerChannel(Channel channel, long streamId) {
    if (streamStates.containsKey(streamId)) {
      streamStates.get(streamId).associatedChannel = channel;
    }
  }

  @Override
  public ManagedBuffer getChunk(long streamId, String chunkId) {
    StreamState state = streamStates.get(streamId);
    String appId = state.appId;
    String executorId = state.executorId;

    return chunkGetter.getChunk(appId, executorId, chunkId);
  }

  @Override
  public ManagedBuffer openStream(String streamChunkId) {
    Tuple2<Long, String> streamIdAndchunkId = parseStreamChunkId(streamChunkId);
    return getChunk(streamIdAndchunkId._1, streamIdAndchunkId._2);
  }

  public static String genStreamChunkId(long streamId, String chunkId) {
    return String.format("%d-%d", streamId, chunkId);
  }

  public static Tuple2<Long, String> parseStreamChunkId(String streamchunkId) {
    String[] array = streamchunkId.split("-");
    assert array.length == 2:
      "Stream id and chunk index should be specified when open stream for fetching block.";
    long streamId = Long.valueOf(array[0]);
    String chunkId = array[1];
    return new Tuple2<>(streamId, chunkId);
  }

  @Override
  public void connectionTerminated(Channel channel) {
    // Close all streams which have been associated with the channel.
    for (Map.Entry<Long, StreamState> entry: streamStates.entrySet()) {
      StreamState state = entry.getValue();
      if (state.associatedChannel == channel) {
        streamStates.remove(entry.getKey());
      }
    }
  }

  @Override
  public void checkAuthorization(TransportClient client, long streamId) {
    if (client.getClientId() != null) {
      StreamState state = streamStates.get(streamId);
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

  /**
   * Register metadata(appId and executorId) in a stream. Thus client doesn't need to send the
   * metadata again when fetch chunks.
   */
  public long registerStream(String appId, String executorId) {
    long myStreamId = nextStreamId.getAndIncrement();
    streamStates.put(myStreamId, new StreamState(appId, executorId));
    return myStreamId;
  }

}
