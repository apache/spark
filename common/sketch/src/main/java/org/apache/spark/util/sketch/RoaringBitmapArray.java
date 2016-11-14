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

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

class RoaringBitmapArray {

    private final RoaringBitmap[] data;
    private long bitCount; // number of 1`s in bitset
    private final long numBits; // total number of available bits

    private static int numOfBuckets(long numBits) {
        if (numBits <= 0) {
            throw new IllegalArgumentException("numBits must be positive, but got " + numBits);
        }
        return (int) Math.ceil(numBits / (double)Integer.MAX_VALUE);
    }

    private static RoaringBitmap[] initialVector(int numOfBuckets){
        RoaringBitmap[] vector = new RoaringBitmap[numOfBuckets];
        for(int i = 0;i<numOfBuckets;i++){
            vector[i] = new RoaringBitmap();
        }
        return vector;
    }

    RoaringBitmapArray(long numBits) {
        this(initialVector(numOfBuckets(numBits)), numBits);
    }

    private RoaringBitmapArray(RoaringBitmap[] data, long numBits) {
        this.data = data;
        this.numBits = numBits;
        long bitCount = 0;
        for (RoaringBitmap bucket : data) {
            bitCount += bucket.getCardinality();
        }
        this.bitCount = bitCount;
    }

    /** Returns true if the bit changed value. */
    boolean set(long index) {
        int bucketNum = (int)(index / Integer.MAX_VALUE);
        int bitIdx = (int)(index % Integer.MAX_VALUE);
        if(!data[bucketNum].contains(bitIdx)) {
            data[bucketNum].add(bitIdx);
            bitCount++;
            return true;
        }
        return false;
    }

    boolean get(long index) {
        int bucketNum = (int)(index / Integer.MAX_VALUE);
        int bitIdx = (int) (index % Integer.MAX_VALUE);
        return data[bucketNum].contains(bitIdx);
    }

    /** Number of bits */
    long bitSize() {
        return numBits;
    }

    /** Number of set bits (1s) */
    long cardinality() {
        return bitCount;
    }

    /** Combines the two RoaringBitmapArray using bitwise OR. */
    void putAll(RoaringBitmapArray bitmap) {
        assert data.length == bitmap.data.length : "RoaringBitmapArray`s must be of equal length when merging";
        long mergedBitCount = 0;
        for (int i = 0; i < data.length; i++) {
            data[i].or(bitmap.data[i]);
            mergedBitCount += data[i].getCardinality();
        }
        this.bitCount = mergedBitCount;
    }

    void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(data.length);
        out.writeLong(numBits);
        for (RoaringBitmap datum : data) {
            datum.runOptimize();
            datum.serialize(out);
        }
    }

    static RoaringBitmapArray readFrom(DataInputStream in) throws IOException {
        int numOfBuckets = in.readInt();
        long numBits = in.readLong();
        RoaringBitmap[] data = new RoaringBitmap[numOfBuckets];
        for (int i = 0; i < numOfBuckets; i++) {
            data[i] = new RoaringBitmap();
            data[i].deserialize(in);
        }
        return new RoaringBitmapArray(data, numBits);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || !(other instanceof RoaringBitmapArray)) return false;
        RoaringBitmapArray that = (RoaringBitmapArray) other;
        return (this.bitCount == that.bitCount) &&
                (this.numBits == that.numBits) &&
                 Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

}
