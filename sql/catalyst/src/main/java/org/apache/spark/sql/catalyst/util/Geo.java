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

import java.nio.ByteOrder;

// Helper interface for the APIs expected from top-level GEOMETRY and GEOGRAPHY classes.
interface Geo {

  // The default endianness for in-memory geo objects is Little Endian (NDR).
  ByteOrder DEFAULT_ENDIANNESS = ByteOrder.LITTLE_ENDIAN;

  /**
   * In-memory representation of a geospatial object:
   * +------------------------------+
   * |          Geo Object          |
   * +------------------------------+
   * | Header          (Fixed Size) |
   * | +--------------------------+ |
   * | | SRID           [4 bytes] | |
   * | +--------------------------+ |
   * |                              |
   * | Payload      (Variable Size) |
   * | +--------------------------+ |
   * | | WKB         [byte array] | |
   * | +--------------------------+ |
   * +------------------------------+
   * Byte order: Little Endian (NDR).
   */

  // Constant defining the size of the header in the geo object in-memory representation.
  int HEADER_SIZE = 4;

  /** Header offsets. */

  // Constant defining the offset of the Spatial Reference System Identifier (SRID) value.
  int SRID_OFFSET = 0;

  /** Payload offsets. */

  // Constant defining the offset of the Well-Known Binary (WKB) representation value.
  int WKB_OFFSET = HEADER_SIZE;

  /** Binary converters. */

  // Returns the Well-Known Binary (WKB) representation of the geo object.
  byte[] toWkb();
  byte[] toWkb(ByteOrder endianness);

  // Returns the Extended Well-Known Binary (EWKB) representation of the geo object.
  byte[] toEwkb();
  byte[] toEwkb(ByteOrder endianness);

  /** Textual converters. */

  // Returns the Well-Known Text (WKT) representation of the geo object.
  byte[] toWkt();

  // Returns the Extended Well-Known Text (EWKT) representation of the geo object.
  byte[] toEwkt();

  /** Other methods. */

  // Returns the Spatial Reference Identifier (SRID) value of the geo object.
  int srid();

  // Sets the Spatial Reference Identifier (SRID) value of the geo object.
  void setSrid(int srid);

}
