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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FetchShuffleBlocksSuite {

  @Test
  public void testFetchShuffleBlockEncodeDecode() {
    FetchShuffleBlocks fetchShuffleBlocks =
      new FetchShuffleBlocks("app0", "exec1", 0, new long[] {0}, new int[][] {{0, 1}}, false);
    Assertions.assertEquals(2, fetchShuffleBlocks.getNumBlocks());
    int len = fetchShuffleBlocks.encodedLength();
    Assertions.assertEquals(50, len);
    ByteBuf buf = Unpooled.buffer(len);
    fetchShuffleBlocks.encode(buf);

    FetchShuffleBlocks decoded = FetchShuffleBlocks.decode(buf);
    assertEquals(fetchShuffleBlocks, decoded);
  }
}
