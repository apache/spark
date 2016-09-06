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

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.buffer.ChunkedByteBufferUtil;

/** Verifies that all BlockTransferMessages can be serialized correctly. */
public class BlockTransferMessagesSuite {
  @Test
  public void serializeOpenShuffleBlocks() throws IOException {
    checkSerializeDeserialize(new OpenBlocks("app-1", "exec-2", new String[]{"b1", "b2"}));
    checkSerializeDeserialize(new RegisterExecutor("app-1", "exec-2", new ExecutorShuffleInfo(
        new String[]{"/local1", "/local2"}, 32, "MyShuffleManager")));
    checkSerializeDeserialize(new UploadBlock("app-1", "exec-2", "block-3", new byte[]{1, 2},
        new NioManagedBuffer(ChunkedByteBufferUtil.wrap(new byte[]{4, 5, 6, 7}))));
    checkSerializeDeserialize(new StreamHandle(12345, 16));
  }

  private void checkSerializeDeserialize(BlockTransferMessage msg) throws IOException {
    BlockTransferMessage msg2 = BlockTransferMessage.Decoder.
        fromByteBuffer(msg.toByteBuffer());
    assertEquals(msg, msg2);
    assertEquals(msg.hashCode(), msg2.hashCode());
    assertEquals(msg.toString(), msg2.toString());
  }
}
