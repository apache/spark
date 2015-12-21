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

import io.netty.channel.Channel;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * The StreamManager is used to fetch individual chunks from a stream. This is used in
 * {@link TransportRequestHandler} in order to respond to fetchChunk() requests. Creation of the
 * stream is outside the scope of the transport layer, but a given stream is guaranteed to be read
 * by only one client connection, meaning that getChunk() for a particular stream will be called
 * serially and that once the connection associated with the stream is closed, that stream will
 * never be used again.
 */
public abstract class StreamManager {
  /**
   * Called in response to a fetchChunk() request. The returned buffer will be passed as-is to the
   * client. A single stream will be associated with a single TCP connection, so this method
   * will not be called in parallel for a particular stream.
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   *
   * The returned ManagedBuffer will be release()'d after being written to the network.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * Called in response to a stream() request. The returned data is streamed to the client
   * through a single TCP connection.
   *
   * Note the <code>streamId</code> argument is not related to the similarly named argument in the
   * {@link #getChunk(long, int)} method.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @return A managed buffer for the stream, or null if the stream was not found.
   */
  public ManagedBuffer openStream(String streamId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Associates a stream with a single client connection, which is guaranteed to be the only reader
   * of the stream. The getChunk() method will be called serially on this connection and once the
   * connection is closed, the stream will never be used again, enabling cleanup.
   *
   * This must be called before the first getChunk() on the stream, but it may be invoked multiple
   * times with the same channel and stream id.
   */
  public void registerChannel(Channel channel, long streamId) { }

  /**
   * Indicates that the given channel has been terminated. After this occurs, we are guaranteed not
   * to read from the associated streams again, so any state can be cleaned up.
   */
  public void connectionTerminated(Channel channel) { }

  /**
   * Verify that the client is authorized to read from the given stream.
   *
   * @throws SecurityException If client is not authorized.
   */
  public void checkAuthorization(TransportClient client, long streamId) { }

}
