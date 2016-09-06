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

package org.apache.spark.network.shuffle.protocol;

import java.io.IOException;
import java.io.InputStream;

import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.network.buffer.ChunkedByteBufferOutputStream;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver;
import org.apache.spark.network.shuffle.protocol.mesos.ShuffleServiceHeartbeat;

/**
 * Messages handled by the {@link org.apache.spark.network.shuffle.ExternalShuffleBlockHandler}, or
 * by Spark's NettyBlockTransferService.
 *
 * At a high level:
 *   - OpenBlock is handled by both services, but only services shuffle files for the external
 *     shuffle service. It returns a StreamHandle.
 *   - UploadBlock is only handled by the NettyBlockTransferService.
 *   - RegisterExecutor is only handled by the external shuffle service.
 */
public abstract class BlockTransferMessage implements Encodable {
  protected abstract Type type();

  /** Preceding every serialized message is its type, which allows us to deserialize it. */
  public enum Type {
    OPEN_BLOCKS(0), UPLOAD_BLOCK(1), REGISTER_EXECUTOR(2), STREAM_HANDLE(3), REGISTER_DRIVER(4),
    HEARTBEAT(5);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }
  }

  // NB: Java does not support static methods in interfaces, so we must put this in a static class.
  public static class Decoder {

    /** Deserializes the 'type' byte followed by the message itself. */
    public static BlockTransferMessage fromByteBuffer(ChunkedByteBuffer buf) throws IOException {
      return fromDataInputStream(buf.toInputStream());
    }

    public static BlockTransferMessage fromDataInputStream(InputStream in) throws IOException {
      byte type = Encoders.Bytes.decode(in);
      switch (type) {
        case 0: return OpenBlocks.decode(in);
        case 1: return UploadBlock.decode(in);
        case 2: return RegisterExecutor.decode(in);
        case 3: return StreamHandle.decode(in);
        case 4: return RegisterDriver.decode(in);
        case 5: return ShuffleServiceHeartbeat.decode(in);
        default: throw new IllegalArgumentException("Unknown message type: " + type);
      }
    }
  }

  /** Serializes the 'type' byte followed by the message itself. */
  public ChunkedByteBuffer toByteBuffer() {
    try {
      ChunkedByteBufferOutputStream out = ChunkedByteBufferOutputStream.newInstance();
      // Allow room for encoded message, plus the type byte
      Encoders.Bytes.encode(out, type().id);
      encode(out);
      out.close();
      return out.toChunkedByteBuffer();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }
}
