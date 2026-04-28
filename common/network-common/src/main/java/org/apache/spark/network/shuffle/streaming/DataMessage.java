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

package org.apache.spark.network.shuffle.streaming;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

public final class DataMessage extends StreamingShuffleMessage {

    public ByteBuf data;
    public int shuffleWriterId;
    public int shuffleReaderId;
    public int dataSize;
    public long checksum;

    public DataMessage(int shuffleWriterId, int shuffleReaderId, int dataSize, ByteBuf data,
                       long checksum) {
        this.shuffleWriterId = shuffleWriterId;
        this.shuffleReaderId = shuffleReaderId;
        this.dataSize = dataSize;
        this.data = data;
        this.ownedBuf = data.retain();
        this.checksum = checksum;
    }

    @Override
    public StreamingShuffleMessageType messageType() {
        return StreamingShuffleMessageType.DATA_MESSAGE_UNSAFE_ROW;
    }

    @Override
    public int headerLength() {
        // 4 bytes EACH for shuffle writer ID, shuffle reader ID, data size
        // 8 bytes for checksum
        return super.headerLength() + 20;
    }

    @Override
    public void encode(CompositeByteBuf buf) {
        super.encode(buf);
        buf.writeInt(shuffleWriterId);
        buf.writeInt(shuffleReaderId);
        buf.writeInt(dataSize);
        buf.writeLong(checksum);

        // Adding data as a component to buf transfers ownership of data to buf. However,
        // this DataMessage still has a reference to data, so we need to retain it here.
        buf.addComponent(true, data.retain());
    }

    public static DataMessage decode(ByteBuf message) {
        int shuffleWriterId = message.readInt();
        int shuffleReaderId = message.readInt();
        int dataSize = message.readInt();
        long checksum = message.readLong();
        // Validate dataSize at decode time so malformed frames fail at the wire boundary
        // with a clear message, rather than later inside getRecordData().
        if (dataSize < 0 || dataSize > message.readableBytes()) {
            throw new IllegalArgumentException(
                "Invalid DataMessage dataSize=" + dataSize +
                    ", readable bytes after header=" + message.readableBytes());
        }
        return new DataMessage(shuffleWriterId, shuffleReaderId, dataSize, message, checksum);
    }

    /**
     * Returns a slice of {@link #data} containing exactly the serialized records
     * (i.e., {@code dataSize} bytes starting at the current reader index).
     */
    public ByteBuf getRecordData() {
        return data.slice(data.readerIndex(), dataSize);
    }
}
