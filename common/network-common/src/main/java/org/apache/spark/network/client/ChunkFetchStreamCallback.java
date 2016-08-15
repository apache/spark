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

package org.apache.spark.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.spark.network.buffer.*;
import org.apache.spark.network.protocol.StreamChunkId;

public class ChunkFetchStreamCallback implements StreamCallback {
  private final StreamChunkId streamChunkId;
  private final ChunkReceivedCallback listener;
  private final ChunkedByteBufferOutputStream outputStream = new
      ChunkedByteBufferOutputStream(32 * 1024);
  private final WritableByteChannel channel = Channels.newChannel(outputStream);

  public ChunkFetchStreamCallback(
      ChunkReceivedCallback listener,
      StreamChunkId streamChunkId) {
    this.listener = listener;
    this.streamChunkId = streamChunkId;
  }

  public void onData(String streamId, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      int ret = channel.write(buffer);
      if (ret == 0) {
        throw new IOException("Could not fully write buffer to output stream");
      }
    }

  }

  public void onComplete(String streamId) throws IOException {
    channel.close();
    ManagedBuffer body = new NioManagedBuffer(outputStream.toChunkedByteBuffer());
    listener.onSuccess(streamChunkId.chunkIndex, body);
  }

  public void onFailure(String streamId, Throwable cause) throws IOException {
    channel.close();
    listener.onFailure(streamChunkId.chunkIndex, cause);
  }
}
