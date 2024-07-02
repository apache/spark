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

public enum BloomFilterStrategies implements BloomFilterStrategy {
  HASH_32() {
    @Override
    public boolean putString(String item, BitArray bits, int numHashFunctions) {
      return putBinary(Utils.getBytesFromUTF8String(item), bits, numHashFunctions);
    }

    @Override
    public boolean putLong(long item, BitArray bits, int numHashFunctions) {
      // Here we first hash the input long element into 2 int hash values, h1 and h2, then produce n
      // hash values by `h1 + i * h2` with 1 <= i <= numHashFunctions.
      // Note that `CountMinSketch` use a different strategy, it hash the input long element with
      // every i to produce n hash values.
      // TODO: the strategy of `CountMinSketch` looks more advanced, should we follow it here?
      int h1 = Murmur3_x86_32.hashLong(item, 0);
      int h2 = Murmur3_x86_32.hashLong(item, h1);

      long bitSize = bits.bitSize();
      boolean bitsChanged = false;
      for (int i = 1; i <= numHashFunctions; i++) {
        int combinedHash = h1 + (i * h2);
        // Flip all the bits if it's negative (guaranteed positive number)
        if (combinedHash < 0) {
          combinedHash = ~combinedHash;
        }
        bitsChanged |= bits.set(combinedHash % bitSize);
      }
      return bitsChanged;
    }

    @Override
    public boolean putBinary(byte[] item, BitArray bits, int numHashFunctions) {
      int h1 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
      int h2 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, h1);

      long bitSize = bits.bitSize();
      boolean bitsChanged = false;
      for (int i = 1; i <= numHashFunctions; i++) {
        int combinedHash = h1 + (i * h2);
        // Flip all the bits if it's negative (guaranteed positive number)
        if (combinedHash < 0) {
          combinedHash = ~combinedHash;
        }
        bitsChanged |= bits.set(combinedHash % bitSize);
      }
      return bitsChanged;
    }

    @Override
    public boolean mightContainString(String item, BitArray bits, int numHashFunctions) {
      return mightContainBinary(Utils.getBytesFromUTF8String(item), bits, numHashFunctions);
    }

    @Override
    public boolean mightContainLong(long item, BitArray bits, int numHashFunctions) {
      int h1 = Murmur3_x86_32.hashLong(item, 0);
      int h2 = Murmur3_x86_32.hashLong(item, h1);

      long bitSize = bits.bitSize();
      for (int i = 1; i <= numHashFunctions; i++) {
        int combinedHash = h1 + (i * h2);
        // Flip all the bits if it's negative (guaranteed positive number)
        if (combinedHash < 0) {
          combinedHash = ~combinedHash;
        }
        if (!bits.get(combinedHash % bitSize)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean mightContainBinary(byte[] item, BitArray bits, int numHashFunctions) {
      int h1 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
      int h2 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, h1);

      long bitSize = bits.bitSize();
      for (int i = 1; i <= numHashFunctions; i++) {
        int combinedHash = h1 + (i * h2);
        // Flip all the bits if it's negative (guaranteed positive number)
        if (combinedHash < 0) {
          combinedHash = ~combinedHash;
        }
        if (!bits.get(combinedHash % bitSize)) {
          return false;
        }
      }
      return true;
    }
  },
  HASH_128() {
    @Override
    public boolean putString(String item, BitArray bits, int numHashFunctions) {
      return putBinary(Utils.getBytesFromUTF8String(item), bits, numHashFunctions);
    }

    @Override
    public boolean putLong(long item, BitArray bits, int numHashFunctions) {
      long bitSize = bits.bitSize();
      Murmur3_x86_128.HashObject hashObject = Murmur3_x86_128.hashLong(item, 0);
      long hash1 = hashObject.getHash1();
      long hash2 = hashObject.getHash2();

      boolean bitsChanged = false;
      long combinedHash = hash1;
      for (int i = 0; i < numHashFunctions; i++) {
        // Make the combined hash positive and indexable
        bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
        combinedHash += hash2;
      }
      return bitsChanged;
    }

    @Override
    public boolean putBinary(byte[] item, BitArray bits, int numHashFunctions) {
      long bitSize = bits.bitSize();
      Murmur3_x86_128.HashObject hashObject = Murmur3_x86_128.hashUnsafeBytes(
        item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
      long hash1 = hashObject.getHash1();
      long hash2 = hashObject.getHash2();

      boolean bitsChanged = false;
      long combinedHash = hash1;
      for (int i = 0; i < numHashFunctions; i++) {
        // Make the combined hash positive and indexable
        bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
        combinedHash += hash2;
      }
      return bitsChanged;
    }

    @Override
    public boolean mightContainString(String item, BitArray bits, int numHashFunctions) {
      return mightContainBinary(Utils.getBytesFromUTF8String(item), bits, numHashFunctions);
    }

    @Override
    public boolean mightContainLong(long item, BitArray bits, int numHashFunctions) {
      long bitSize = bits.bitSize();
      Murmur3_x86_128.HashObject hashObject = Murmur3_x86_128.hashLong(item, 0);
      long hash1 = hashObject.getHash1();
      long hash2 = hashObject.getHash2();

      long combinedHash = hash1;
      for (int i = 0; i < numHashFunctions; i++) {
        // Make the combined hash positive and indexable
        if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
          return false;
        }
        combinedHash += hash2;
      }
      return true;
    }

    @Override
    public boolean mightContainBinary(byte[] item, BitArray bits, int numHashFunctions) {
      long bitSize = bits.bitSize();
      Murmur3_x86_128.HashObject hashObject = Murmur3_x86_128.hashUnsafeBytes(
        item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
      long hash1 = hashObject.getHash1();
      long hash2 = hashObject.getHash2();

      long combinedHash = hash1;
      for (int i = 0; i < numHashFunctions; i++) {
        // Make the combined hash positive and indexable
        if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
          return false;
        }
        combinedHash += hash2;
      }
      return true;
    }
  };
}

