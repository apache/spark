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

import org.apache.spark.unsafe.types.GeometryVal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

// Catalyst-internal server-side execution wrapper for GEOMETRY.
public final class Geometry implements Geo {

  /** Geometry internal implementation. */

  // The Geometry type is implemented as an array of bytes stored inside a `GeometryVal` object.
  protected final GeometryVal value;

  /** Geometry constants. */

  // The default SRID value for GEOMETRY values.
  public static int DEFAULT_SRID = 0;

  /** Geometry constructors and factory methods. */

  // We make the constructors private. Use `fromBytes` or `fromValue` to create new instances.
  private Geometry(byte[] bytes) {
    this.value = GeometryVal.fromBytes(bytes);
  }

  private Geometry(GeometryVal value) {
    this.value = value;
  }

  // Factory methods to create new Geometry instances from a byte array or a `GeometryVal`.
  public static Geometry fromBytes(byte[] bytes) {
    return new Geometry(bytes);
  }

  public static Geometry fromValue(GeometryVal value) {
    return new Geometry(value);
  }

  /** Geometry getters and instance methods. */

  // Returns the underlying physical type value of this Geometry instance.
  public GeometryVal getValue() {
    return value;
  }

  // Returns the byte array containing the GEOMETRY representation/encoding.
  public byte[] getBytes() {
    return value.getBytes();
  }

  // Returns a copy of this geometry.
  public Geometry copy() {
    byte[] bytes = getBytes();
    return Geometry.fromBytes(Arrays.copyOf(bytes, bytes.length));
  }

  /** Geometry WKB parsing. */

  // Returns a Geometry object with the specified SRID value by parsing the input WKB.
  public static Geometry fromWkb(byte[] wkb, int srid) {
    byte[] bytes = new byte[HEADER_SIZE + wkb.length];
    ByteBuffer.wrap(bytes).order(DEFAULT_ENDIANNESS).putInt(srid);
    System.arraycopy(wkb, 0, bytes, WKB_OFFSET, wkb.length);
    return fromBytes(bytes);
  }

  // Overload for the WKB reader where we use the default SRID for Geometry.
  public static Geometry fromWkb(byte[] wkb) {
    return fromWkb(wkb, DEFAULT_SRID);
  }

  /** Geometry EWKB parsing. */

  // Returns a Geometry object by parsing the input EWKB.
  public static Geometry fromEwkb(byte[] ewkb) {
    throw new UnsupportedOperationException("Geometry EWKB parsing is not yet supported.");
  }

  /** Geometry WKT parsing. */

  // Returns a Geometry object with the specified SRID value by parsing the input WKT.
  public static Geometry fromWkt(byte[] wkt, int srid) {
    throw new UnsupportedOperationException("Geometry WKT parsing is not yet supported.");
  }

  // Overload for the WKT reader where we use the default SRID for Geometry.
  public static Geometry fromWkt(byte[] wkt) {
    return fromWkt(wkt, DEFAULT_SRID);
  }

  /** Geometry EWKT parsing. */

  // Returns a Geometry object by parsing the input EWKT.
  public static Geometry fromEwkt(byte[] ewkt) {
    throw new UnsupportedOperationException("Geometry EWKT parsing is not yet supported.");
  }

  /** Geometry binary standard format converters: WKB and EWKB. */

  @Override
  public byte[] toWkb() {
    // This method returns only the WKB portion of the in-memory Geometry representation.
    // Note that the header is skipped, and that the WKB is returned as-is (little-endian).
    return Arrays.copyOfRange(getBytes(), WKB_OFFSET, getBytes().length);
  }

  @Override
  public byte[] toWkb(ByteOrder endianness) {
    // The default endianness is Little Endian (NDR).
    if (endianness == DEFAULT_ENDIANNESS) {
      return toWkb();
    } else {
      throw new UnsupportedOperationException("Geometry WKB endianness is not yet supported.");
    }
  }

  @Override
  public byte[] toEwkb() {
    throw new UnsupportedOperationException("Geometry EWKB conversion is not yet supported.");
  }

  @Override
  public byte[] toEwkb(ByteOrder endianness) {
    throw new UnsupportedOperationException("Geometry EWKB endianness is not yet supported.");
  }

  /** Geometry textual standard format converters: WKT and EWKT. */

  @Override
  public byte[] toWkt() {
    // Once WKT conversion is implemented, it should support various precisions.
    throw new UnsupportedOperationException("Geometry WKT conversion is not yet supported.");
  }

  @Override
  public byte[] toEwkt() {
    // Once EWKT conversion is implemented, it should support various precisions.
    throw new UnsupportedOperationException("Geometry EWKT conversion is not yet supported.");
  }

  /** Other instance methods, inherited from the `Geo` interface. */

  @Override
  public int srid() {
    // This method gets the SRID value from the in-memory Geometry representation header.
    return getWrapper().getInt(SRID_OFFSET);
  }

  @Override
  public void setSrid(int srid) {
    // This method sets the SRID value in the in-memory Geometry representation header.
    getWrapper().putInt(SRID_OFFSET, srid);
  }

  /** Other private helper/utility methods used for implementation. */

  // Returns a byte buffer wrapper over the byte buffer of this geometry value.
  private ByteBuffer getWrapper() {
    return ByteBuffer.wrap(getBytes()).order(DEFAULT_ENDIANNESS);
  }

}
