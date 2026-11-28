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

import java.io.*;

class BloomFilterImplV2 extends BloomFilterBase implements Serializable {

  // The value Java computed before V3, so that V2 filters an earlier version serialized with Java
  // serialization still read back.
  @Serial
  private static final long serialVersionUID = -7609404176825362195L;

  BloomFilterImplV2(int numHashFunctions, long numBits, int seed) {
    this(new BitArray(numBits), numHashFunctions, seed);
  }

  private BloomFilterImplV2(BitArray bits, int numHashFunctions, int seed) {
    super(bits, numHashFunctions, seed);
  }

  BloomFilterImplV2() {}

  /** The binary format version this class writes and reads. */
  Version version() {
    return Version.V2;
  }

  protected boolean scatterHashAndSetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();

    long bitSize = bits.bitSize();
    boolean bitsChanged = false;

    // Integer.MAX_VALUE takes care of scrambling the higher four bytes of combinedHash
    long combinedHash = (long) h1 * Integer.MAX_VALUE;
    for (long i = 0; i < numHashFunctions; i++) {
      combinedHash += h2;

      // Flip all the bits if it's negative (guaranteed positive number)
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;

      bitsChanged |= bits.set(combinedIndex % bitSize);
    }
    return bitsChanged;
  }

  protected boolean scatterHashAndGetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();

    long bitSize = bits.bitSize();

    // Integer.MAX_VALUE takes care of scrambling the higher four bytes of combinedHash
    long combinedHash = (long) h1 * Integer.MAX_VALUE;
    for (long i = 0; i < numHashFunctions; i++) {
      combinedHash += h2;

      // Flip all the bits if it's negative (guaranteed positive number)
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;

      if (!bits.get(combinedIndex % bitSize)) {
        return false;
      }
    }
    return true;
  }

  protected BloomFilterImplV2 checkCompatibilityForMerge(BloomFilter other)
          throws IncompatibleMergeException {
    // Duplicates the logic of `isCompatible` here to provide better error message.
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    // A V3 filter is a V2 one with other bit positions, so the classes must match.
    if (other.getClass() != getClass()) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }
    BloomFilterImplV2 that = (BloomFilterImplV2) other;

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.seed != that.seed) {
      throw new IncompatibleMergeException(
              "Cannot merge bloom filters with different seeds"
      );
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filters with different number of hash functions"
      );
    }
    return that;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);

    dos.writeInt(version().getVersionNumber());
    dos.writeInt(numHashFunctions);
    dos.writeInt(seed);
    bits.writeTo(dos);
  }

  void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != version().getVersionNumber()) {
      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
    }

    this.numHashFunctions = dis.readInt();
    this.seed = dis.readInt();
    this.bits = BitArray.readFrom(dis);
  }

  public static BloomFilterImplV2 readFrom(InputStream in) throws IOException {
    BloomFilterImplV2 filter = new BloomFilterImplV2();
    filter.readFrom0(in);
    return filter;
  }

  @Serial
  private void writeObject(ObjectOutputStream out) throws IOException {
    writeTo(out);
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException {
    readFrom0(in);
  }
}
