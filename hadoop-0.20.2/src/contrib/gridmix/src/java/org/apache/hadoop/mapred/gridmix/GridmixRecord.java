/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

class GridmixRecord implements WritableComparable<GridmixRecord> {

  private static final int FIXED_BYTES = 1;
  private int size = -1;
  private long seed;
  private final DataInputBuffer dib =
    new DataInputBuffer();
  private final DataOutputBuffer dob =
    new DataOutputBuffer(Long.SIZE / Byte.SIZE);
  private byte[] literal = dob.getData();

  GridmixRecord() {
    this(1, 0L);
  }

  GridmixRecord(int size, long seed) {
    this.seed = seed;
    setSizeInternal(size);
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    setSizeInternal(size);
  }

  private void setSizeInternal(int size) {
    this.size = Math.max(1, size);
    try {
      seed = maskSeed(seed, this.size);
      dob.reset();
      dob.writeLong(seed);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public final void setSeed(long seed) {
    this.seed = seed;
  }

  /** Marsaglia, 2003. */
  long nextRand(long x) {
    x ^= (x << 13);
    x ^= (x >>> 7);
    return (x ^= (x << 17));
  }

  public void writeRandom(DataOutput out, final int size) throws IOException {
    long tmp = seed;
    out.writeLong(tmp);
    int i = size - (Long.SIZE / Byte.SIZE);
    while (i > Long.SIZE / Byte.SIZE - 1) {
      tmp = nextRand(tmp);
      out.writeLong(tmp);
      i -= Long.SIZE / Byte.SIZE;
    }
    for (tmp = nextRand(tmp); i > 0; --i) {
      out.writeByte((int)(tmp & 0xFF));
      tmp >>>= Byte.SIZE;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    size = WritableUtils.readVInt(in);
    int payload = size - WritableUtils.getVIntSize(size);
    if (payload > Long.SIZE / Byte.SIZE) {
      seed = in.readLong();
      payload -= Long.SIZE / Byte.SIZE;
    } else {
      Arrays.fill(literal, (byte)0);
      in.readFully(literal, 0, payload);
      dib.reset(literal, 0, literal.length);
      seed = dib.readLong();
      payload = 0;
    }
    final int vBytes = in.skipBytes(payload);
    if (vBytes != payload) {
      throw new EOFException("Expected " + payload + ", read " + vBytes);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // data bytes including vint encoding
    WritableUtils.writeVInt(out, size);
    final int payload = size - WritableUtils.getVIntSize(size);
    if (payload > Long.SIZE / Byte.SIZE) {
      writeRandom(out, payload);
    } else if (payload > 0) {
      out.write(literal, 0, payload);
    }
  }

  @Override
  public int compareTo(GridmixRecord other) {
    return compareSeed(other.seed,
        Math.max(0, other.getSize() - other.fixedBytes()));
  }

  int fixedBytes() {
    // min vint size
    return FIXED_BYTES;
  }

  private static long maskSeed(long sd, int sz) {
    // Don't use fixedBytes here; subclasses will set intended random len
    if (sz <= FIXED_BYTES) {
      sd = 0L;
    } else if (sz < Long.SIZE / Byte.SIZE + FIXED_BYTES) {
      final int tmp = sz - FIXED_BYTES;
      final long mask = (1L << (Byte.SIZE * tmp)) - 1;
      sd &= mask << (Byte.SIZE * (Long.SIZE / Byte.SIZE - tmp));
    }
    return sd;
  }

  int compareSeed(long jSeed, int jSize) {
    final int iSize = Math.max(0, getSize() - fixedBytes());
    final int seedLen = Math.min(iSize, jSize) + FIXED_BYTES;
    jSeed = maskSeed(jSeed, seedLen);
    long iSeed = maskSeed(seed, seedLen);
    final int cmplen = Math.min(iSize, jSize);
    for (int i = 0; i < cmplen; i += Byte.SIZE) {
      final int k = cmplen - i;
      for (long j = Long.SIZE - Byte.SIZE;
          j >= Math.max(0, Long.SIZE / Byte.SIZE - k) * Byte.SIZE;
          j -= Byte.SIZE) {
        final int xi = (int)((iSeed >>> j) & 0xFFL);
        final int xj = (int)((jSeed >>> j) & 0xFFL);
        if (xi != xj) {
          return xi - xj;
        }
      }
      iSeed = nextRand(iSeed);
      jSeed = nextRand(jSeed);
    }
    return iSize - jSize;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other != null && other.getClass() == getClass()) {
      final GridmixRecord o = ((GridmixRecord)other);
      return getSize() == o.getSize() && seed == o.seed;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int)(seed * getSize());
  }

  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(GridmixRecord.class);
    }

    public Comparator(Class<? extends WritableComparable<?>> sub) {
      super(sub);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      n1 -= WritableUtils.getVIntSize(n1);
      n2 -= WritableUtils.getVIntSize(n2);
      return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
    }

    static {
      WritableComparator.define(GridmixRecord.class, new Comparator());
    }
  }

}
