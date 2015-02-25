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

public interface LargeByteBuffer {
    public long capacity();

    public byte get();

    public void get(byte[] dst,int offset, int length);

    public void position(long position);

    public long position();

    /** doesn't copy data, just copies references & offsets */
    public LargeByteBuffer duplicate();

    public void put(LargeByteBuffer bytes);

    public long remaining();

    //TODO checks on limit semantics

    /**
     * Sets this buffer's limit. If the position is larger than the new limit then it is set to the
     * new limit. If the mark is defined and larger than the new limit then it is discarded.
     */
    public void limit(long newLimit);

    /**
     * return this buffer's limit
     * @return
     */
    public long limit();

    //an alternative to having this method would be having a foreachBuffer(f: Buffer => T)
    public long writeTo(WritableByteChannel channel) throws IOException;


    //TODO this should be deleted -- just to help me get going
    public ByteBuffer firstByteBuffer();

}
