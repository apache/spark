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

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.StreamInterceptor;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportFrameDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A holder for streamed data sent along with an RPC message.
 */
public class StreamData {

  private final TransportRequestHandler handler;
  private final TransportFrameDecoder frameDecoder;
  private final RpcResponseCallback rpcCallback;
  private final ByteBuffer meta;
  private final long streamByteCount;
  private boolean hasCallback = false;

  public StreamData(
      TransportRequestHandler handler,
      TransportFrameDecoder frameDecoder,
      RpcResponseCallback rpcCallback,
      ByteBuffer meta,
      long streamByteCount) {
    this.handler = handler;
    this.frameDecoder = frameDecoder;
    this.rpcCallback = rpcCallback;
    this.meta = meta;
    this.streamByteCount = streamByteCount;
  }

  public boolean hasCallback() {
    return hasCallback;
  }

  /**
   * Register callback to receive the streaming data.
   *
   * If an exception is thrown from the callback, it will be propogated back to the sender as an rpc
   * failure.
   * @param callback
   */
  public void registerStreamCallback(String streamId, StreamCallback callback) throws IOException {
    if (hasCallback) {
      throw new IllegalStateException("Cannot register more than one stream callback");
    }
    hasCallback = true;
    // the passed callback handles the actual data, but we need to also make sure we respond to the
    // original rpc request.
    StreamCallback wrappedCallback = new StreamCallback() {
      @Override
      public void onData(String streamId, ByteBuffer buf) throws IOException {
        callback.onData(streamId, buf);
      }

      @Override
      public void onComplete(String streamId) throws IOException {
        callback.onComplete(streamId);
        rpcCallback.onSuccess(ByteBuffer.allocate(0));
      }

      @Override
      public void onFailure(String streamId, Throwable cause) throws IOException {
        rpcCallback.onFailure(new IOException("Destination failed while reading stream", cause));
        callback.onFailure(streamId, cause);
      }
    };
    if (streamByteCount > 0) {
      StreamInterceptor interceptor = new StreamInterceptor(handler, streamId, streamByteCount,
          wrappedCallback);
      frameDecoder.setInterceptor(interceptor);
    } else {
      wrappedCallback.onComplete(streamId);
    }
  }
}
