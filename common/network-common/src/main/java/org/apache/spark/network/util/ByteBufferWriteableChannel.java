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

package org.apache.spark.network.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

public class ByteBufferWriteableChannel implements WritableByteChannel {
    private final ByteBuffer destination;
    private boolean open;

    public ByteBufferWriteableChannel(ByteBuffer destination) {
        this.destination = destination;
        this.open = true;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!isOpen()) {
            throw new ClosedChannelException();
        }
        int bytesToWrite = Math.min(src.remaining(), destination.remaining());
        // Destination buffer is full
        if (bytesToWrite == 0) {
            return 0;
        }
        ByteBuffer temp = src.slice().limit(bytesToWrite);
        destination.put(temp);
        src.position(src.position() + bytesToWrite);
        return bytesToWrite;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
