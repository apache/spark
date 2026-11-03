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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.types.variant.Variant;

import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;

/**
 * The physical data representation of {@link org.apache.spark.sql.types.VariantType} that
 * represents a semi-structured value. It consists of two binary values: {@link VariantVal#value}
 * and {@link VariantVal#metadata}. The value encodes types and values, but not field names. The
 * metadata currently contains a version flag and a list of field names. We can extend/modify the
 * detailed binary format given the version flag.
 * <p>
 * A {@link VariantVal} can be produced by casting another value into the Variant type or parsing a
 * JSON string in the {@link org.apache.spark.sql.catalyst.expressions.variant.ParseJson}
 * expression. We can extract a path consisting of field names and array indices from it, cast it
 * into a concrete data type, or rebuild a JSON string from it.
 * <p>
 * The storage layout of this class in {@link org.apache.spark.sql.catalyst.expressions.UnsafeRow}
 * and {@link org.apache.spark.sql.catalyst.expressions.UnsafeArrayData} is: the fixed-size part is
 * a long value "offsetAndSize". The upper 32 bits is the offset that points to the start position
 * of the actual binary content. The lower 32 bits is the total length of the binary content. The
 * binary content contains: 4 bytes representing the length of {@link VariantVal#value}, 1 flags
 * byte (see {@link VariantVal#CANONICAL_FLAG}), content of {@link VariantVal#value}, content of
 * {@link VariantVal#metadata}. This is an internal and transient format and can be modified at any
 * time.
 */
public class VariantVal implements Serializable {
  /** Size of the header before the value bytes in the UnsafeRow binary content: length + flags. */
  public static final int UNSAFE_ROW_HEADER_SIZE = 5;
  /** Bit of the flags byte that records {@link VariantVal#canonical}. */
  public static final byte CANONICAL_FLAG = 0x1;

  protected final byte[] value;
  protected final byte[] metadata;
  /**
   * Whether `value` and `metadata` are known to be in canonical form, so that logically equal
   * variants have identical bytes. The canonicalization expression sets it so that later
   * canonicalizations of the same value in the query can return immediately. It survives UnsafeRow
   * round trips (and therefore shuffle, sort and spill) but is dropped by every other
   * representation, where it resets to `false`. It is not part of equality.
   */
  protected final boolean canonical;

  public VariantVal(byte[] value, byte[] metadata) {
    this(value, metadata, false);
  }

  public VariantVal(byte[] value, byte[] metadata, boolean canonical) {
    this.value = value;
    this.metadata = metadata;
    this.canonical = canonical;
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getMetadata() {
    return metadata;
  }

  public boolean isCanonical() {
    return canonical;
  }

  /** The flags byte stored in the UnsafeRow binary content for this value. */
  public byte unsafeRowFlags() {
    return canonical ? CANONICAL_FLAG : 0;
  }

  /**
   * This function reads the binary content described in `writeIntoUnsafeRow` from `baseObject`. The
   * offset is computed by adding the offset in {@code offsetAndSize} and {@code baseOffset}.
   */
  public static VariantVal readFromUnsafeRow(
      long offsetAndSize,
      Object baseObject,
      long baseOffset) {
    // offset and totalSize is the upper/lower 32 bits in offsetAndSize.
    int offset = (int) (offsetAndSize >> 32);
    int totalSize = (int) offsetAndSize;
    int valueSize = Platform.getInt(baseObject, baseOffset + offset);
    byte flags = Platform.getByte(baseObject, baseOffset + offset + 4);
    int metadataSize = totalSize - UNSAFE_ROW_HEADER_SIZE - valueSize;
    byte[] value = new byte[valueSize];
    byte[] metadata = new byte[metadataSize];
    Platform.copyMemory(
        baseObject,
        baseOffset + offset + UNSAFE_ROW_HEADER_SIZE,
        value,
        Platform.BYTE_ARRAY_OFFSET,
        valueSize
    );
    Platform.copyMemory(
        baseObject,
        baseOffset + offset + UNSAFE_ROW_HEADER_SIZE + valueSize,
        metadata,
        Platform.BYTE_ARRAY_OFFSET,
        metadataSize
    );
    return new VariantVal(value, metadata, (flags & CANONICAL_FLAG) != 0);
  }

  public String debugString() {
    return "VariantVal{" +
        "value=" + Arrays.toString(value) +
        ", metadata=" + Arrays.toString(metadata) +
        '}';
  }

  public String toJson(ZoneId zoneId) {
    return new Variant(value, metadata).toJson(zoneId);
  }

  /**
   * @return A human-readable representation of the Variant value. It is always a JSON string at
   * this moment.
   */
  @Override
  public String toString() {
    return toJson(ZoneOffset.UTC);
  }

  /**
   * Compare two variants in bytes. The variant equality is more complex than it, and we haven't
   * supported it in the user surface yet. This method is only intended for tests.
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof VariantVal o) {
      return Arrays.equals(value, o.value) && Arrays.equals(metadata, o.metadata);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(value);
    result = 31 * result + Arrays.hashCode(metadata);
    return result;
  }
}
