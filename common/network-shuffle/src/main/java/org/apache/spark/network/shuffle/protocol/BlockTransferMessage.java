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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.shuffle.ExternalBlockHandler;

/**
 * Messages handled by the {@link ExternalBlockHandler}, or
 * by Spark's NettyBlockTransferService.
 *
 * At a high level:
 *   - OpenBlock is logically only handled by the NettyBlockTransferService, but for the capability
 *     for old version Spark, we still keep it in external shuffle service.
 *     It returns a StreamHandle.
 *   - UploadBlock is only handled by the NettyBlockTransferService.
 *   - RegisterExecutor is only handled by the external shuffle service.
 *   - RemoveBlocks is only handled by the external shuffle service.
 *   - FetchShuffleBlocks is handled by both services for shuffle files. It returns a StreamHandle.
 */
public abstract class BlockTransferMessage implements Encodable {
  protected abstract Type type();

  /** Preceding every serialized message is its type, which allows us to deserialize it. */
  public enum Type {
    OPEN_BLOCKS(0), UPLOAD_BLOCK(1), REGISTER_EXECUTOR(2), STREAM_HANDLE(3), REGISTER_DRIVER(4),
    HEARTBEAT(5), UPLOAD_BLOCK_STREAM(6), REMOVE_BLOCKS(7), BLOCKS_REMOVED(8),
    FETCH_SHUFFLE_BLOCKS(9), GET_LOCAL_DIRS_FOR_EXECUTORS(10), LOCAL_DIRS_FOR_EXECUTORS(11),
    PUSH_BLOCK_STREAM(12), FINALIZE_SHUFFLE_MERGE(13), MERGE_STATUSES(14),
    FETCH_SHUFFLE_BLOCK_CHUNKS(15), DIAGNOSE_CORRUPTION(16), CORRUPTION_CAUSE(17),
    PUSH_BLOCK_RETURN_CODE(18), REMOVE_SHUFFLE_MERGE(19);

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
    public static BlockTransferMessage fromByteBuffer(ByteBuffer msg) {
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      byte type = buf.readByte();
      return switch (type) {
        case 0 -> OpenBlocks.decode(buf);
        case 1 -> UploadBlock.decode(buf);
        case 2 -> RegisterExecutor.decode(buf);
        case 3 -> StreamHandle.decode(buf);
        case 6 -> UploadBlockStream.decode(buf);
        case 7 -> RemoveBlocks.decode(buf);
        case 8 -> BlocksRemoved.decode(buf);
        case 9 -> FetchShuffleBlocks.decode(buf);
        case 10 -> GetLocalDirsForExecutors.decode(buf);
        case 11 -> LocalDirsForExecutors.decode(buf);
        case 12 -> PushBlockStream.decode(buf);
        case 13 -> FinalizeShuffleMerge.decode(buf);
        case 14 -> MergeStatuses.decode(buf);
        case 15 -> FetchShuffleBlockChunks.decode(buf);
        case 16 -> DiagnoseCorruption.decode(buf);
        case 17 -> CorruptionCause.decode(buf);
        case 18 -> BlockPushReturnCode.decode(buf);
        case 19 -> RemoveShuffleMerge.decode(buf);
        default -> throw new IllegalArgumentException("Unknown message type: " + type);
      };
    }
  }

  /** Serializes the 'type' byte followed by the message itself. */
  public ByteBuffer toByteBuffer() {
    // Allow room for encoded message, plus the type byte
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    buf.writeByte(type().id);
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }
}
