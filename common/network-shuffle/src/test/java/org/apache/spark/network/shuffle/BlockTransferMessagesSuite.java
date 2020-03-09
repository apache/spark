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

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.network.shuffle.protocol.*;

/** Verifies that all BlockTransferMessages can be serialized correctly. */
public class BlockTransferMessagesSuite {
  @Test
  public void serializeOpenShuffleBlocks() {
    checkSerializeDeserialize(new OpenBlocks("app-1", "exec-2", new String[] { "b1", "b2" }));
    checkSerializeDeserialize(new FetchShuffleBlocks(
      "app-1", "exec-2", 0, new long[] {0, 1},
      new int[][] {{ 0, 1 }, { 0, 1, 2 }}, false));
    checkSerializeDeserialize(new FetchShuffleBlocks(
      "app-1", "exec-2", 0, new long[] {0, 1},
      new int[][] {{ 0, 1 }, { 0, 2 }}, true));
    checkSerializeDeserialize(new RegisterExecutor("app-1", "exec-2", new ExecutorShuffleInfo(
      new String[] { "/local1", "/local2" }, 32, "MyShuffleManager")));
    checkSerializeDeserialize(new UploadBlock("app-1", "exec-2", "block-3", new byte[] { 1, 2 },
      new byte[] { 4, 5, 6, 7} ));
    checkSerializeDeserialize(new StreamHandle(12345, 16));
  }

  @Test
  public void testLocalDirsMessages() {
    checkSerializeDeserialize(
      new GetLocalDirsForExecutors("app-1", new String[]{"exec-1", "exec-2"}));

    Map<String, String[]> map = new HashMap<>();
    map.put("exec-1", new String[]{"loc1.1"});
    map.put("exec-22", new String[]{"loc2.1", "loc2.2"});
    LocalDirsForExecutors localDirsForExecs = new LocalDirsForExecutors(map);
    Map<String, String[]> resultMap =
      ((LocalDirsForExecutors)checkSerializeDeserialize(localDirsForExecs)).getLocalDirsByExec();
    assertEquals(resultMap.size(), map.keySet().size());
    for (Map.Entry<String, String[]> e: map.entrySet()) {
      assertTrue(resultMap.containsKey(e.getKey()));
      assertArrayEquals(e.getValue(), resultMap.get(e.getKey()));
    }
  }

  private BlockTransferMessage checkSerializeDeserialize(BlockTransferMessage msg) {
    BlockTransferMessage msg2 = BlockTransferMessage.Decoder.fromByteBuffer(msg.toByteBuffer());
    assertEquals(msg, msg2);
    assertEquals(msg.hashCode(), msg2.hashCode());
    assertEquals(msg.toString(), msg2.toString());
    return msg2;
  }
}
