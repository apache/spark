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
import java.nio.channels.WritableByteChannel;

public class WrappedLargeByteBuffer implements LargeByteBuffer {

    final ByteBuffer[] underlying;
    private final Long totalCapacity;
    private final long[] chunkOffsets;

    private long _pos;
    private int currentBufferIdx;
    private ByteBuffer currentBuffer;
    private long limit;


    public WrappedLargeByteBuffer(ByteBuffer[] underlying) {
        this.underlying = underlying;
        long sum = 0l;
        chunkOffsets = new long[underlying.length];
        for (int i = 0; i < underlying.length; i++) {
            chunkOffsets[i] = sum;
            sum += underlying[i].capacity();
        }
        totalCapacity = sum;
        _pos = 0l;
        currentBufferIdx = 0;
        currentBuffer = underlying[0];
        limit = totalCapacity;
    }

    @Override
    public long capacity() {return totalCapacity;}

    @Override
    public void get(byte[] dest, int offset, int length){
        int moved = 0;
        while (moved < length) {
            int toRead = Math.min(length - moved, currentBuffer.remaining());
            currentBuffer.get(dest, offset, toRead);
            moved += toRead;
            updateCurrentBuffer();
        }
    }

    @Override
    public byte get() {
        byte r = currentBuffer.get();
        _pos += 1;
        updateCurrentBuffer();
        return r;
    }

    private void updateCurrentBuffer() {
        //TODO fix end condition
        while(!currentBuffer.hasRemaining()) {
            currentBufferIdx += 1;
            currentBuffer = underlying[currentBufferIdx];
        }
    }

    @Override
    public void put(LargeByteBuffer bytes) {
        throw new RuntimeException("not yet implemented");
    }

    @Override
    public long position() { return _pos;}

    @Override
    public void position(long newPosition) {
        //XXX check range?
        _pos = newPosition;
    }

    @Override
    public long remaining() {
        return limit - _pos;
    }

    @Override
    public WrappedLargeByteBuffer duplicate() {
        ByteBuffer[] duplicates = new ByteBuffer[underlying.length];
        for (int i = 0; i < underlying.length; i++) {
            duplicates[i] = underlying[i].duplicate();
        }
        //we could also avoid initializing offsets here, if we cared ...
        return new WrappedLargeByteBuffer(duplicates);
    }

//    @Override
//    public void rewind() {
//        _pos = 0;
//        for (ByteBuffer buf: underlying) {
//            buf.rewind();
//        }
//    }

    @Override
    public long limit() {
        return limit;
    }

    @Override
    public void limit(long newLimit) {
        //XXX check range?  set limits in sub buffers?
        limit = newLimit;
    }

    @Override
    public long writeTo(WritableByteChannel channel) throws IOException {
        long written = 0l;
        for(ByteBuffer buffer: underlying) {
            //TODO test this
            //XXX do we care about respecting the limit here?
            written += buffer.remaining();
            while (buffer.hasRemaining())
                channel.write(buffer);
        }
        return written;
    }

    @Override
    public ByteBuffer firstByteBuffer() {
        return underlying[0];
    }
}
