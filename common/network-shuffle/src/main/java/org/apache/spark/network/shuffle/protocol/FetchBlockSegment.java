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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.protocol.ResponseMessage;

import com.google.common.base.Objects;

import io.netty.buffer.ByteBuf;

/**
 * Request to fetch a sequence of a single chunk of a stream. This will correspond to a single
 * {@link ResponseMessage} (either success or failure).
 */
public final class FetchBlockSegment extends BlockTransferMessage {
    public final String blockId;
    public final long offset;
    public final int length;

    public FetchBlockSegment(String blockId, long offset, int length) {
        this.blockId = blockId;
        this.offset=offset;
        this.length=length;
    }

    @Override
    public Type type() { return Type.FETCH_BLOCK_SEGMENT; }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(blockId)+ 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf,blockId);
        buf.writeLong(offset);
        buf.writeInt(length);
    }

    public static FetchBlockSegment decode(ByteBuf buf) {
        String blockId=Encoders.Strings.decode(buf);
        long offset=buf.readLong();
        int length=buf.readInt();
        return new FetchBlockSegment(blockId,offset,length);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(blockId,offset,length);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof FetchBlockSegment) {
            FetchBlockSegment o = (FetchBlockSegment) other;
            return blockId.equals(o.blockId) && offset==o.offset&& length==o.length;
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("blockId", blockId)
                .append("offset",offset).append("length",length)
                .toString();
    }
}