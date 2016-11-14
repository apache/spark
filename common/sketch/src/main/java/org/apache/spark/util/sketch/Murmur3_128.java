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

package org.apache.spark.util.sketch;

/**
 * 128-bit Murmur3 hasher.
 * Best perfomance is on x86_64 platform
 * Based on implementation https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
 */
final class Murmur3_128 {

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    static void hashBytes(byte[] data, long seed, long[] hashes) {
        hash(data, Platform.BYTE_ARRAY_OFFSET, data.length, seed, hashes);
    }

    static void hashLong(long data, long seed, long[] hashes) {
        hash(new long[]{data}, Platform.LONG_ARRAY_OFFSET, 8, seed, hashes); // 8 - long`s size in bytes
    }

    @SuppressWarnings("fallthrough")
    private static void hash(Object key, int offset, int length, long seed, long[] result) {
        long h1 = seed & 0x00000000FFFFFFFFL;
        long h2 = seed & 0x00000000FFFFFFFFL;

        int roundedEnd = offset + (length & 0xFFFFFFF0); // round down to 16 byte block
        for (int i=offset; i<roundedEnd; i+=16) {
            long k1 = getLongLittleEndian(key, i);
            long k2 = getLongLittleEndian(key, i + 8);

            h1 ^= mixK1(k1);

            h1 = Long.rotateLeft(h1,27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            h2 ^= mixK2(k2);

            h2 = Long.rotateLeft(h2,31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15: k2  = (Platform.getByte(key, roundedEnd + 14) & 0xFFL) << 48; // fall through
            case 14: k2 |= (Platform.getByte(key, roundedEnd + 13) & 0xFFL) << 40; // fall through
            case 13: k2 |= (Platform.getByte(key, roundedEnd + 12) & 0xFFL) << 32; // fall through
            case 12: k2 |= (Platform.getByte(key, roundedEnd + 11) & 0xFFL) << 24; // fall through
            case 11: k2 |= (Platform.getByte(key, roundedEnd + 10) & 0xFFL) << 16; // fall through
            case 10: k2 |= (Platform.getByte(key, roundedEnd +  9) & 0xFFL) << 8; // fall through
            case 9:  k2 |= (Platform.getByte(key, roundedEnd +  8) & 0xFFL); // fall through
                h2 ^= mixK2(k2);
            case 8: k1  = ((long)Platform.getByte(key, roundedEnd + 7))   << 56; // fall through
            case 7: k1 |= (Platform.getByte(key, roundedEnd + 6) & 0xFFL) << 48; // fall through
            case 6: k1 |= (Platform.getByte(key, roundedEnd + 5) & 0xFFL) << 40; // fall through
            case 5: k1 |= (Platform.getByte(key, roundedEnd + 4) & 0xFFL) << 32; // fall through
            case 4: k1 |= (Platform.getByte(key, roundedEnd + 3) & 0xFFL) << 24; // fall through
            case 3: k1 |= (Platform.getByte(key, roundedEnd + 2) & 0xFFL) << 16; // fall through
            case 2: k1 |= (Platform.getByte(key, roundedEnd + 1) & 0xFFL) << 8; // fall through
            case 1: k1 |= (Platform.getByte(key, roundedEnd) & 0xFFL);
                h1 ^= mixK1(k1);
        }
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        result[0] = h1;
        result[1] = h2;
    }

    /**
     * Gets a long from a byte buffer in little endian byte order.
     */
    private static long getLongLittleEndian(Object key, int offset) {
        return ( Platform.getByte(key, offset) & 0xFFL)
                | ((Platform.getByte(key, offset + 1) & 0xFFL) << 8)
                | ((Platform.getByte(key, offset + 2) & 0xFFL) << 16)
                | ((Platform.getByte(key, offset + 3) & 0xFFL) << 24)
                | ((Platform.getByte(key, offset + 4) & 0xFFL) << 32)
                | ((Platform.getByte(key, offset + 5) & 0xFFL) << 40)
                | ((Platform.getByte(key, offset + 6) & 0xFFL) << 48)
                | (((long) Platform.getByte(key, offset + 7) ) << 56);
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long mixK1(long k1) {
        k1 *= C1;
        k1 = Long.rotateLeft(k1,31);
        k1 *= C2;
        return k1;
    }

    private static long mixK2(long k2) {
        k2 *= C2;
        k2 = Long.rotateLeft(k2,33);
        k2 *= C1;
        return k2;
    }

}