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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.shuffle.protocol.*;

import java.nio.ByteBuffer;

/** Verifies that all BlockTransferMessages can be serialized correctly. */
public class BlockTransferMessagesSuite {
  @Test
  public void serializeOpenShuffleBlocks() {
    checkSerializeDeserialize(new OpenBlocks("app-1", "exec-2", new String[] { "b1", "b2" }, false));
    checkSerializeDeserialize(new RegisterExecutor("app-1", "exec-2", new ExecutorShuffleInfo(
      new String[] { "/local1", "/local2" }, 32, "MyShuffleManager")));
    checkSerializeDeserialize(new UploadBlock("app-1", "exec-2", "block-3", new byte[] { 1, 2 },
      new byte[] { 4, 5, 6, 7} ));
    checkSerializeDeserialize(new StreamHandle(12345, 16, new int[0]));
  }

  private void checkSerializeDeserialize(BlockTransferMessage msg) {
    BlockTransferMessage msg2 = BlockTransferMessage.Decoder.fromByteBuffer(msg.toByteBuffer());
    assertEquals(msg, msg2);
    assertEquals(msg.hashCode(), msg2.hashCode());
    assertEquals(msg.toString(), msg2.toString());
  }

  private BlockTransferMessage testFromByteBuffer(ByteBuffer msg) {
    ByteBuf buf = Unpooled.wrappedBuffer(msg);
    byte type = buf.readByte();
    switch (type) {
      case 0: return TestOpenBlocks.decode(buf);
      case 3: return TestStreamHandle.decode(buf);
      default: throw new IllegalArgumentException("Unknown message type: " + type);
    }
  }

  private void checkOpenBlocks(OpenBlocks ob1, TestOpenBlocks ob2) {
    assertEquals(ob1.appId, ob2.appId);
    assertEquals(ob1.execId, ob2.execId);
    assertArrayEquals(ob1.blockIds, ob2.blockIds);
  }

  @Test
  public void checkOpenBlocksBackwardCompatibility() {
    TestOpenBlocks ob1 = new TestOpenBlocks("app-1", "exec-2", new String[] { "b1", "b2" });
    OpenBlocks ob2 = (OpenBlocks)BlockTransferMessage.Decoder.fromByteBuffer(ob1.toByteBuffer());
    checkOpenBlocks(ob2, ob1);
    assertEquals(ob2.shuffleBlockBatchFetch, false);

    OpenBlocks ob3 = new OpenBlocks("app-1", "exec-2", new String[] { "b1", "b2" }, true);
    TestOpenBlocks ob4 = (TestOpenBlocks)testFromByteBuffer(ob3.toByteBuffer());
    checkOpenBlocks(ob3, ob4);
  }

  private void checkStreamHandle(StreamHandle sh1, TestStreamHandle sh2) {
    assertEquals(sh1.streamId, sh2.streamId);
    assertEquals(sh1.numChunks, sh2.numChunks);
  }

  @Test
  public void checkStreamHandleBackwardCompatibility() {
    TestStreamHandle sh1 = new TestStreamHandle(12345, 16);
    StreamHandle sh2 = (StreamHandle)BlockTransferMessage.Decoder.fromByteBuffer(sh1.toByteBuffer());
    checkStreamHandle(sh2, sh1);
    assertArrayEquals(sh2.chunkSizes, new int[0]);

    StreamHandle sh3 = new StreamHandle(12345, 16, new int[0]);
    TestStreamHandle sh4 = (TestStreamHandle)testFromByteBuffer(sh3.toByteBuffer());
    checkStreamHandle(sh3, sh4);
  }

}
