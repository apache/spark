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
package org.apache.spark.sql.catalyst.util;

import org.apache.spark.sql.catalyst.util.geo.GeometryModel;
import org.apache.spark.sql.catalyst.util.geo.WkbParseException;
import org.apache.spark.sql.catalyst.util.geo.WkbReader;
import org.apache.spark.sql.catalyst.util.geo.WkbWriter;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.unsafe.types.BinaryView;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

// Catalyst-internal server-side execution wrapper for GEOGRAPHY.
public final class Geography implements Geo {

  /** Geography internal implementation. */

  // The GEOGRAPHY physical value is an opaque chunk of bytes, carried as a {@link BinaryView}.
  protected final BinaryView value;

  /** Geography constants. */

  // The default SRID value for GEOGRAPHY values.
  public static int DEFAULT_SRID = 4326;

  /** Geography constructors and factory methods. */

  // We make the constructors private. Use `fromBytes` or `fromValue` to create new instances.
  private Geography(byte[] bytes) {
    this.value = BinaryView.fromBytes(bytes);
  }

  private Geography(BinaryView value) {
    this.value = value;
  }

  // Factory methods to create new Geography instances from a byte array or a {@link BinaryView}.
  public static Geography fromBytes(byte[] bytes) {
    return new Geography(bytes);
  }

  public static Geography fromValue(BinaryView value) {
    return new Geography(value);
  }

  /** Geography getters and instance methods. */

  // Returns the underlying physical-type value of this Geography instance.
  public BinaryView getValue() {
    return value;
  }

  // Returns the byte array containing the GEOGRAPHY representation/encoding.
  public byte[] getBytes() {
    return value.getBytes();
  }

  // Returns a copy of this geography that owns its own backing buffer. Required before
  // calling mutating methods like {@link #setSrid(int)} on a value that was read directly
  // from an UnsafeRow / ColumnVector buffer.
  public Geography copy() {
    return new Geography(value.copy());
  }

  /** Geography WKB parsing. */

  // Returns a Geography object with the specified SRID value by parsing the input WKB.
  public static Geography fromWkb(byte[] wkb, int srid) {
    try {
      WkbReader reader = new WkbReader(true);
      reader.read(wkb); // Validate WKB with geography coordinate bounds.

      byte[] bytes = new byte[HEADER_SIZE + wkb.length];
      ByteBuffer.wrap(bytes).order(DEFAULT_ENDIANNESS).putInt(srid);
      System.arraycopy(wkb, 0, bytes, WKB_OFFSET, wkb.length);
      return fromBytes(bytes);
    } catch (WkbParseException e) {
      throw QueryExecutionErrors.wkbParseError(e.getParseError(), e.getPosition());
    }
  }

  // Overload for the WKB reader where we use the default SRID for Geography.
  public static Geography fromWkb(byte[] wkb) {
    return fromWkb(wkb, DEFAULT_SRID);
  }

  /** Geography EWKB parsing. */

  // Returns a Geography object by parsing the input EWKB.
  public static Geography fromEwkb(byte[] ewkb) {
    throw new UnsupportedOperationException("Geography EWKB parsing is not yet supported.");
  }

  /** Geography WKT parsing. */

  // Returns a Geography object with the specified SRID value by parsing the input WKT.
  public static Geography fromWkt(byte[] wkt, int srid) {
    throw new UnsupportedOperationException("Geography WKT parsing is not yet supported.");
  }

  // Overload for the WKT reader where we use the default SRID for Geography.
  public static Geography fromWkt(byte[] wkt) {
    return fromWkt(wkt, DEFAULT_SRID);
  }

  /** Geography EWKT parsing. */

  // Returns a Geography object by parsing the input EWKT.
  public static Geography fromEwkt(byte[] ewkt) {
    throw new UnsupportedOperationException("Geography EWKT parsing is not yet supported.");
  }

  /** Geography binary standard format converters: WKB and EWKB. */

  @Override
  public byte[] toWkb(ByteOrder endianness) {
    return toWkbInternal(endianness);
  }

  private byte[] toWkbInternal(ByteOrder endianness) {
    WkbReader reader = new WkbReader(true);
    GeometryModel model = reader.read(Arrays.copyOfRange(
      getBytes(), WKB_OFFSET, getBytes().length));
    WkbWriter writer = new WkbWriter();
    return writer.write(model, endianness);
  }

  @Override
  public byte[] toEwkb() {
    throw new UnsupportedOperationException("Geography EWKB conversion is not yet supported.");
  }

  @Override
  public byte[] toEwkb(ByteOrder endianness) {
    throw new UnsupportedOperationException("Geography EWKB endianness is not yet supported.");
  }

  /** Geography textual standard format converters: WKT and EWKT. */

  @Override
  public byte[] toWkt() {
    return toWktInternal().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public byte[] toEwkt() {
    String ewkt = "SRID=" + srid() + ";" + toWktInternal();
    return ewkt.getBytes(StandardCharsets.UTF_8);
  }

  private String toWktInternal() {
    WkbReader reader = new WkbReader(true);
    GeometryModel model = reader.read(Arrays.copyOfRange(
      getBytes(), WKB_OFFSET, getBytes().length));
    return model.toString();
  }

  /** Other instance methods, inherited from the `Geo` interface. */

  @Override
  public int srid() {
    // This method gets the SRID value from the in-memory Geography representation header.
    return getWrapper().getInt(SRID_OFFSET);
  }

  @Override
  public void setSrid(int srid) {
    // This method sets the SRID value in the in-memory Geography representation header.
    // It mutates the backing buffer in place, which only writes through when this value owns
    // a tight, on-heap array. For a sliced / sub-range / off-heap view, getBytes() (and hence
    // getWrapper()) returns a throwaway copy and the write would be silently lost, so fail
    // loudly and direct the caller to copy() first.
    if (!value.hasTightOnHeapArray()) {
      throw new IllegalStateException(
        "setSrid requires a Geography that owns its backing buffer; call copy() before mutating "
          + "a value read directly from an UnsafeRow / ColumnVector buffer.");
    }
    getWrapper().putInt(SRID_OFFSET, srid);
  }

  /** Other private helper/utility methods used for implementation. */

  // Returns a byte buffer wrapper over the byte buffer of this geography value.
  private ByteBuffer getWrapper() {
    return ByteBuffer.wrap(getBytes()).order(DEFAULT_ENDIANNESS);
  }

}
