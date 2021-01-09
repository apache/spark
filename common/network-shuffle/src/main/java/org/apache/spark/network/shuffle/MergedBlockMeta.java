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
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.roaringbitmap.RoaringBitmap;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.protocol.Encoders;

/**
 * Contains meta information for a merged block. Currently this information constitutes:
 * 1. Number of chunks in a merged shuffle block.
 * 2. Bitmaps for each chunk in the merged block. A chunk bitmap contains all the mapIds that were
 *    merged to that merged block chunk.
 *
 * @since 3.1.0
 */
public class MergedBlockMeta {
  private final int numChunks;
  private final ManagedBuffer chunksBitmapBuffer;

  public MergedBlockMeta(int numChunks, ManagedBuffer chunksBitmapBuffer) {
    this.numChunks = numChunks;
    this.chunksBitmapBuffer = Preconditions.checkNotNull(chunksBitmapBuffer);
  }

  public int getNumChunks() {
    return numChunks;
  }

  public ManagedBuffer getChunksBitmapBuffer() {
    return chunksBitmapBuffer;
  }

  public RoaringBitmap[] readChunkBitmaps() throws IOException {
    ByteBuf buf = Unpooled.wrappedBuffer(chunksBitmapBuffer.nioByteBuffer());
    List<RoaringBitmap> bitmaps = new ArrayList<>();
    while(buf.isReadable()) {
      bitmaps.add(Encoders.Bitmaps.decode(buf));
    }
    assert (bitmaps.size() == numChunks);
    return bitmaps.toArray(new RoaringBitmap[0]);
  }
}
