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
import java.util.List;

public interface LargeByteBuffer {
    public byte get();

    public void get(byte[] dst,int offset, int length);

    public void position(long position);

    public long position();

    /** doesn't copy data, just copies references & offsets */
    public LargeByteBuffer duplicate();

    public void put(LargeByteBuffer bytes);

    public long remaining();

    /**
     * the total number of bytes in this buffer
     * @return
     */
    public long size();

    /**
     * writes the entire contents of this buffer to the given channel
     *
     * @param channel
     * @return
     * @throws IOException
     */
    public long writeTo(WritableByteChannel channel) throws IOException;

    /**
     * get the entire contents of this as one ByteBuffer, if possible.  The returned ByteBuffer
     * will always have the position set 0, and the limit set to the end of the data.  Each
     * call will return a new ByteBuffer, but will not require copying the data (eg., it will
     * use ByteBuffer#duplicate()). Updates to the ByteBuffer will be reflected in this
     * LargeByteBuffer.
     *
     * @return
     * @throws BufferTooLargeException if this buffer is too large to fit in one {@link ByteBuffer}
     */
    public ByteBuffer asByteBuffer() throws BufferTooLargeException;

    /**
     * Attempt to clean up if it is memory-mapped. This uses an *unsafe* Sun API that
     * might cause errors if one attempts to read from the unmapped buffer, but it's better than
     * waiting for the GC to find it because that could lead to huge numbers of open files. There's
     * unfortunately no standard API to do this.
     */
    public void dispose();

    //List b/c we need to know the size.  Could also use Iterator w/ separate numBuffers method
    //TODO delete
    public List<ByteBuffer> nioBuffers();

}
