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

package org.apache.spark.unsafe.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.apache.spark.annotation.Unstable;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

/**
 * A non-owning view over a contiguous chunk of bytes that may live on-heap or off-heap.
 * It is intended as the shared physical carrier for opaque-bytes SQL types whose values
 * can be read directly out of an {@code UnsafeRow} / {@code UnsafeArrayData} /
 * {@code ColumnVector} backing buffer.
 * <p>
 * Lifetime: a {@code BinaryView} is only valid for as long as the memory it points to is
 * alive. Callers that need to retain a value across the source buffer's lifetime must call
 * {@link #copy()} first.
 */
@Unstable
public final class BinaryView implements Comparable<BinaryView>, Externalizable, KryoSerializable {

  // null when off-heap; a byte[] (or other primitive array) when on-heap.
  private Object base;
  // For on-heap, this is BYTE_ARRAY_OFFSET + index into the array. For off-heap, this is
  // the absolute native address.
  private long offset;
  private int numBytes;

  /** For Externalizable / KryoSerializable only. */
  public BinaryView() {}

  private BinaryView(Object base, long offset, int numBytes) {
    this.base = base;
    this.offset = offset;
    this.numBytes = numBytes;
  }

  // ---------- factories ----------

  /**
   * Creates a view over the given byte array. The array is referenced, not copied; callers
   * must not mutate it while the returned view is in use.
   */
  public static BinaryView fromBytes(byte[] bytes) {
    if (bytes == null) return null;
    return new BinaryView(bytes, BYTE_ARRAY_OFFSET, bytes.length);
  }

  /** Creates a view over a sub-range of the given byte array (no copy). */
  public static BinaryView fromBytes(byte[] bytes, int offset, int numBytes) {
    if (bytes == null) return null;
    return new BinaryView(bytes, BYTE_ARRAY_OFFSET + offset, numBytes);
  }

  /**
   * Creates a view at the given Tungsten-style address. {@code base == null} means off-heap
   * and {@code offset} is the absolute native address; otherwise {@code base} is a JVM
   * primitive array and {@code offset} is {@code BYTE_ARRAY_OFFSET + index}.
   */
  public static BinaryView fromAddress(Object base, long offset, int numBytes) {
    return new BinaryView(base, offset, numBytes);
  }

  // ---------- accessors ----------

  /** The backing object: a primitive array when on-heap, or {@code null} when off-heap. */
  public Object getBaseObject() { return base; }

  /** Tungsten-style offset: see the class javadoc. */
  public long getBaseOffset() { return offset; }

  public int numBytes() { return numBytes; }

  public boolean isOffHeap() { return base == null; }

  // ---------- random-access primitive reads ----------
  // Coordinates are relative to the start of this view, i.e. i in [0, numBytes).

  public byte getByte(int i) {
    assert i >= 0 && i < numBytes : invalidRangeMessage(i, 1);
    return Platform.getByte(base, offset + i);
  }

  public short getShort(int i) {
    assert i >= 0 && i + 2 <= numBytes : invalidRangeMessage(i, 2);
    return Platform.getShort(base, offset + i);
  }

  public int getInt(int i) {
    assert i >= 0 && i + 4 <= numBytes : invalidRangeMessage(i, 4);
    return Platform.getInt(base, offset + i);
  }

  public long getLong(int i) {
    assert i >= 0 && i + 8 <= numBytes : invalidRangeMessage(i, 8);
    return Platform.getLong(base, offset + i);
  }

  public float getFloat(int i) {
    assert i >= 0 && i + 4 <= numBytes : invalidRangeMessage(i, 4);
    return Platform.getFloat(base, offset + i);
  }

  public double getDouble(int i) {
    assert i >= 0 && i + 8 <= numBytes : invalidRangeMessage(i, 8);
    return Platform.getDouble(base, offset + i);
  }

  private String invalidRangeMessage(int i, int width) {
    return "Invalid access at offset " + i + " (width " + width + ") in BinaryView of "
      + numBytes + " bytes";
  }

  // ---------- materialization and slicing ----------

  /**
   * Returns the bytes of this view as a {@code byte[]}.
   * <p>
   * Mirrors {@link UTF8String#getBytes()}: if this view already owns a tight, on-heap byte
   * array (offset is exactly {@code BYTE_ARRAY_OFFSET} and length equals the array length),
   * the backing array is returned directly without copying. Otherwise a fresh array is
   * allocated and the bytes are copied into it.
   * <p>
   * The caller must not mutate the returned array, since when this view owns a tight array
   * it is shared with the view itself. Use {@link #copy()} to obtain an independent owned
   * value.
   */
  public byte[] getBytes() {
    if (offset == BYTE_ARRAY_OFFSET
        && base instanceof byte[] bytes
        && bytes.length == numBytes) {
      return bytes;
    }
    byte[] out = new byte[numBytes];
    Platform.copyMemory(base, offset, out, BYTE_ARRAY_OFFSET, numBytes);
    return out;
  }

  /**
   * Returns an independent {@code BinaryView} that owns a fresh on-heap byte array
   * containing this view's data. Use this before storing the value past the source
   * buffer's lifetime.
   */
  public BinaryView copy() {
    return new BinaryView(copyToNewArray(), BYTE_ARRAY_OFFSET, numBytes);
  }

  private byte[] copyToNewArray() {
    byte[] out = new byte[numBytes];
    Platform.copyMemory(base, offset, out, BYTE_ARRAY_OFFSET, numBytes);
    return out;
  }

  /** Returns a sub-view (no copy). */
  public BinaryView slice(int start, int len) {
    assert start >= 0 && len >= 0 && start + len <= numBytes
      : "Invalid slice start=" + start + " len=" + len + " of view with " + numBytes + " bytes";
    return new BinaryView(base, offset + start, len);
  }

  /**
   * Copies this view's bytes to the given target memory address. Used by writers that
   * already know where the bytes should land (e.g. {@code UnsafeWriter}).
   */
  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(base, offset, target, targetOffset, numBytes);
  }

  /**
   * Wraps this view as a {@link ByteBuffer}. The heap path returns a {@code ByteBuffer.wrap}
   * around the existing array (zero-copy); the off-heap path materializes a fresh array
   * because there is no portable way to expose an off-heap address through the public
   * {@code ByteBuffer} API.
   */
  public ByteBuffer toByteBuffer() {
    if (base instanceof byte[] bytes && offset >= BYTE_ARRAY_OFFSET) {
      long arrayOffset = offset - BYTE_ARRAY_OFFSET;
      if ((long) bytes.length < arrayOffset + numBytes) {
        throw new ArrayIndexOutOfBoundsException();
      }
      return ByteBuffer.wrap(bytes, (int) arrayOffset, numBytes);
    }
    return ByteBuffer.wrap(copyToNewArray());
  }

  // ---------- equality / hashing / ordering ----------

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytes(base, offset, numBytes, 42);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other instanceof BinaryView o) {
      return numBytes == o.numBytes
        && ByteArrayMethods.arrayEquals(base, offset, o.base, o.offset, numBytes);
    }
    return false;
  }

  /** Lexicographic, unsigned byte-wise comparison. */
  @Override
  public int compareTo(BinaryView other) {
    return ByteArray.compareBinary(
      base, offset, numBytes, other.base, other.offset, other.numBytes);
  }

  // ---------- serialization ----------
  // Both paths always materialize an on-heap byte[] on read so that the deserialized view
  // owns its data; senders may pass a view into another buffer.

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(numBytes);
    if (numBytes > 0) {
      if (offset == BYTE_ARRAY_OFFSET && base instanceof byte[] bytes
          && bytes.length == numBytes) {
        out.write(bytes);
      } else {
        out.write(copyToNewArray());
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    int n = in.readInt();
    byte[] bytes = new byte[n];
    in.readFully(bytes);
    this.base = bytes;
    this.offset = BYTE_ARRAY_OFFSET;
    this.numBytes = n;
  }

  @Override
  public void write(Kryo kryo, Output out) {
    out.writeInt(numBytes);
    if (numBytes > 0) {
      if (offset == BYTE_ARRAY_OFFSET && base instanceof byte[] bytes
          && bytes.length == numBytes) {
        out.write(bytes);
      } else {
        out.write(copyToNewArray());
      }
    }
  }

  @Override
  public void read(Kryo kryo, Input in) {
    int n = in.readInt();
    byte[] bytes = new byte[n];
    in.read(bytes);
    this.base = bytes;
    this.offset = BYTE_ARRAY_OFFSET;
    this.numBytes = n;
  }
}
