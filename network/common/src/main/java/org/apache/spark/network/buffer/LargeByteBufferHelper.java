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
package org.apache.spark.network.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class LargeByteBufferHelper {

    public static final int MAX_CHUNK = Integer.MAX_VALUE - 1000000;

    public static LargeByteBuffer asLargeByteBuffer(ByteBuffer buffer) {
        return new WrappedLargeByteBuffer(new ByteBuffer[]{buffer});
    }

    public static LargeByteBuffer asLargeByteBuffer(byte[] bytes) {
        return new WrappedLargeByteBuffer(new ByteBuffer[]{ByteBuffer.wrap(bytes)});
    }

    public static LargeByteBuffer allocate(long size) {
        ArrayList<ByteBuffer> chunks = new ArrayList<ByteBuffer>();
        long remaining = size;
        while (remaining > 0) {
            int nextSize = (int)Math.min(remaining, MAX_CHUNK);
            ByteBuffer next = ByteBuffer.allocate(nextSize);
            remaining -= nextSize;
            chunks.add(next);
        }
        return new WrappedLargeByteBuffer(chunks.toArray(new ByteBuffer[chunks.size()]));
    }


    public static LargeByteBuffer mapFile(
            FileChannel channel,
            FileChannel.MapMode mode,
            long offset,
            long length
    ) throws IOException {
        int maxChunk = MAX_CHUNK;
        ArrayList<Long> offsets = new ArrayList<Long>();
        long curOffset = offset;
        long end = offset + length;
        while (curOffset < end) {
            offsets.add(curOffset);
            int chunkLength = (int) Math.min((end - curOffset), maxChunk);
            curOffset += chunkLength;
        }
        offsets.add(end);
        ByteBuffer[] chunks = new ByteBuffer[offsets.size() - 1];
        for (int i = 0; i< offsets.size() - 1; i++) {
            chunks[i] = channel.map(mode, offsets.get(i), offsets.get(i+ 1) - offsets.get(i));
        }
        return new WrappedLargeByteBuffer(chunks);
    }


}
