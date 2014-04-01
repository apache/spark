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
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparator;

class GridmixKey extends GridmixRecord {
  static final byte REDUCE_SPEC = 0;
  static final byte DATA = 1;

  static final int META_BYTES = 1;

  private byte type;
  private int partition; // NOT serialized
  private Spec spec = new Spec();

  GridmixKey() {
    this(DATA, 1, 0L);
  }
  GridmixKey(byte type, int size, long seed) {
    super(size, seed);
    this.type = type;
    // setting type may change pcnt random bytes
    setSize(size);
  }

  @Override
  public int getSize() {
    switch (type) {
      case REDUCE_SPEC:
        return super.getSize() + spec.getSize() + META_BYTES;
      case DATA:
        return super.getSize() + META_BYTES;
      default:
        throw new IllegalStateException("Invalid type: " + type);
    }
  }

  @Override
  public void setSize(int size) {
    switch (type) {
      case REDUCE_SPEC:
        super.setSize(size - (META_BYTES + spec.getSize()));
        break;
      case DATA:
        super.setSize(size - META_BYTES);
        break;
      default:
        throw new IllegalStateException("Invalid type: " + type);
    }
  }

  /**
   * Partition is not serialized.
   */
  public int getPartition() {
    return partition;
  }
  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getReduceInputRecords() {
    assert REDUCE_SPEC == getType();
    return spec.rec_in;
  }
  public void setReduceInputRecords(long rec_in) {
    assert REDUCE_SPEC == getType();
    final int origSize = getSize();
    spec.rec_in = rec_in;
    setSize(origSize);
  }

  public long getReduceOutputRecords() {
    assert REDUCE_SPEC == getType();
    return spec.rec_out;
  }
  public void setReduceOutputRecords(long rec_out) {
    assert REDUCE_SPEC == getType();
    final int origSize = getSize();
    spec.rec_out = rec_out;
    setSize(origSize);
  }

  public long getReduceOutputBytes() {
    assert REDUCE_SPEC == getType();
    return spec.bytes_out;
  };
  public void setReduceOutputBytes(long b_out) {
    assert REDUCE_SPEC == getType();
    final int origSize = getSize();
    spec.bytes_out = b_out;
    setSize(origSize);
  }

  public byte getType() {
    return type;
  }
  public void setType(byte type) throws IOException {
    final int origSize = getSize();
    switch (type) {
      case REDUCE_SPEC:
      case DATA:
        this.type = type;
        break;
      default:
        throw new IOException("Invalid type: " + type);
    }
    setSize(origSize);
  }

  public void setSpec(Spec spec) {
    assert REDUCE_SPEC == getType();
    final int origSize = getSize();
    this.spec.set(spec);
    setSize(origSize);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    setType(in.readByte());
    if (REDUCE_SPEC == getType()) {
      spec.readFields(in);
    }
  }
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    final byte t = getType();
    out.writeByte(t);
    if (REDUCE_SPEC == t) {
      spec.write(out);
    }
  }
  int fixedBytes() {
    return super.fixedBytes() +
      (REDUCE_SPEC == getType() ? spec.getSize() : 0) + META_BYTES;
  }
  @Override
  public int compareTo(GridmixRecord other) {
    final GridmixKey o = (GridmixKey) other;
    final byte t1 = getType();
    final byte t2 = o.getType();
    if (t1 != t2) {
      return t1 - t2;
    }
    return super.compareTo(other);
  }

  /**
   * Note that while the spec is not explicitly included, changing the spec
   * may change its size, which will affect equality.
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other != null && other.getClass() == getClass()) {
      final GridmixKey o = ((GridmixKey)other);
      return getType() == o.getType() && super.equals(o);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ getType();
  }

  public static class Spec implements Writable {
    long rec_in;
    long rec_out;
    long bytes_out;
    public Spec() { }

    public void set(Spec other) {
      rec_in = other.rec_in;
      bytes_out = other.bytes_out;
      rec_out = other.rec_out;
    }

    public int getSize() {
      return WritableUtils.getVIntSize(rec_in) +
             WritableUtils.getVIntSize(rec_out) +
             WritableUtils.getVIntSize(bytes_out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      rec_in = WritableUtils.readVLong(in);
      rec_out = WritableUtils.readVLong(in);
      bytes_out = WritableUtils.readVLong(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVLong(out, rec_in);
      WritableUtils.writeVLong(out, rec_out);
      WritableUtils.writeVLong(out, bytes_out);
    }
  }

  public static class Comparator extends GridmixRecord.Comparator {

    private final DataInputBuffer di = new DataInputBuffer();
    private final byte[] reset = di.getData();

    public Comparator() {
      super(GridmixKey.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        di.reset(b1, s1, l1);
        final int x1 = WritableUtils.readVInt(di);
        di.reset(b2, s2, l2);
        final int x2 = WritableUtils.readVInt(di);
        final int ret = (b1[s1 + x1] != b2[s2 + x2])
          ? b1[s1 + x1] - b2[s2 + x2]
          : super.compare(b1, s1, x1, b2, s2, x2);
        di.reset(reset, 0, 0);
        return ret;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    static {
      WritableComparator.define(GridmixKey.class, new Comparator());
    }
  }
}

