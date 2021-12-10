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

package org.apache.spark.starshuffle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Base64;

/**
 * This class stores map result file (shuffle file) information.
 */
public class StarMapResultFileInfo {
    private final String location;
    private final long[] partitionLengths;

    public void serialize(ByteBuf buf) {

        ByteBufUtils.writeLengthAndString(buf, location);
        buf.writeInt(partitionLengths.length);
        for (int i = 0; i < partitionLengths.length; i++) {
            buf.writeLong(partitionLengths[i]);
        }
    }

    public static StarMapResultFileInfo deserialize(ByteBuf buf) {
        String location = ByteBufUtils.readLengthAndString(buf);
        int size = buf.readInt();
        long[] partitionLengths = new long[size];
        for (int i = 0; i < size; i++) {
            partitionLengths[i] = buf.readLong();
        }
        return new StarMapResultFileInfo(location, partitionLengths);
    }

    /***
     * This serialize method is faster than json serialization.
     * @return
     */
    public String serializeToString() {
        ByteBuf buf = Unpooled.buffer();
        try {
            serialize(buf);
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        } finally {
            buf.release();
        }
    }

    public static StarMapResultFileInfo deserializeFromString(String str) {
        byte[] bytes = Base64.getDecoder().decode(str);
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
            return deserialize(buf);
        } finally {
            buf.release();
        }
    }

    public StarMapResultFileInfo(String location, long[] partitionLengths) {
        this.location = location;
        this.partitionLengths = partitionLengths;
    }

    public long[] getPartitionLengths() {
        return partitionLengths;
    }

    public String getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return "MapResultFile{" +
                "location='" + location + '\'' +
                ", partitionLengths=" + Arrays.toString(partitionLengths) +
                '}';
    }
}
